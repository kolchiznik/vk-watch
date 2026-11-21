import ipaddress
import json
import socket
import time
from contextlib import asynccontextmanager
from typing import Dict
from urllib.parse import urlencode, urljoin, urlparse

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response, StreamingResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    try:
        yield
    finally:
        await app.state.http_client.aclose()


app = FastAPI(lifespan=lifespan)
rooms: Dict[str, dict] = {}


def get_room(room_id: str):
    if room_id not in rooms:
        rooms[room_id] = {
            "connections": set(),
            "state": {
                "vk_url": "",
                "stream_url": "",
                "is_hls": False,
                "http_headers": {},
                "playing": False,
                "time": 0.0,
                "updated_at": time.time(),
            },
        }
    return rooms[room_id]


def get_current_room_state(room: dict) -> dict:
    state = room["state"]
    current_time = state["time"]
    if state["playing"]:
        current_time += max(0.0, time.time() - state["updated_at"])
    return {
        "stream_url": state["stream_url"],
        "vk_url": state["vk_url"],
        "is_hls": state["is_hls"],
        "http_headers": state["http_headers"],
        "playing": state["playing"],
        "time": current_time,
        "updated_at": state["updated_at"],
    }


def validate_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise HTTPException(status_code=400, detail="Некорректный URL потока")

    try:
        addresses = {entry[4][0] for entry in socket.getaddrinfo(parsed.hostname, None)}
    except (socket.gaierror, ValueError):
        raise HTTPException(status_code=400, detail="Не удалось определить хост потока")
    if not addresses:
        raise HTTPException(status_code=400, detail="Не удалось определить хост потока")

    for address in addresses:
        if not ipaddress.ip_address(address).is_global:
            raise HTTPException(status_code=400, detail="Прокси-ссылка недоступна")


def proxy_url(url: str, user_agent: str, referer: str) -> str:
    return "/api/proxy?" + urlencode({"url": url, "user_agent": user_agent, "referer": referer})


def rewrite_m3u8_playlist(text: str, playlist_url: str, user_agent: str, referer: str) -> str:
    import re

    rewritten_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            rewritten_lines.append(line)
            continue
        if stripped.startswith("#"):
            def replace_uri(match):
                source_uri = match.group(1)
                if source_uri.lower().startswith("data:"):
                    return match.group(0)
                absolute_uri = urljoin(playlist_url, source_uri)
                return f'URI="{proxy_url(absolute_uri, user_agent, referer)}"'

            rewritten_lines.append(re.sub(r'URI="([^"]+)"', replace_uri, stripped, flags=re.IGNORECASE))
        else:
            rewritten_lines.append(proxy_url(urljoin(playlist_url, stripped), user_agent, referer))

    return "\n".join(rewritten_lines) + ("\n" if text.endswith("\n") else "")


def stream_request_headers(user_agent: str, referer: str, request: Request) -> dict:
    headers = {"User-Agent": user_agent[:512], "Referer": referer[:2048]}
    if range_header := request.headers.get("range"):
        headers["Range"] = range_header
    return headers


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", "r", encoding="utf-8") as file:
        return file.read()


@app.get("/api/extract")
async def extract_vk(url: str):
    ydl_opts = {
        "format": "best[height<=720][ext=mp4]/best[height<=720]/best",
        "quiet": True,
        "no_warnings": True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        stream_url = info.get("url")
        if not stream_url:
            raise ValueError("Не удалось получить поток")

        ext = str(info.get("ext", "")).lower()
        protocol = str(info.get("protocol", "")).lower()
        is_hls = ext == "m3u8" or "m3u8" in protocol or ".m3u8" in stream_url.lower()
        extracted_headers = info.get("http_headers") or {}
        proxy_headers = {
            "User-Agent": extracted_headers.get("User-Agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": extracted_headers.get("Referer") or "https://vkvideo.ru/",
        }

        validate_public_url(stream_url)
        return {
            "stream_url": stream_url,
            "title": info.get("title", "Видео ВК"),
            "is_hls": is_hls,
            "ext": ext,
            "protocol": protocol,
            "http_headers": proxy_headers,
        }
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.get("/api/proxy")
async def proxy_video(
    url: str,
    request: Request,
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    referer: str = "https://vkvideo.ru/",
):
    validate_public_url(url)
    headers = stream_request_headers(user_agent, referer, request)
    client: httpx.AsyncClient = request.app.state.http_client

    try:
        response = await client.send(client.build_request("GET", url, headers=headers), stream=True)

        for _ in range(5):
            if not response.is_redirect:
                break
            location = response.headers.get("location")
            base_url = str(response.request.url)
            await response.aclose()
            if not location:
                raise HTTPException(status_code=502, detail="Поток перенаправляет без адреса")
            url = urljoin(base_url, location)
            validate_public_url(url)
            response = await client.send(client.build_request("GET", url, headers=headers), stream=True)
        else:
            await response.aclose()
            raise HTTPException(status_code=502, detail="Слишком много перенаправлений потока")

        if response.is_redirect:
            await response.aclose()
            raise HTTPException(status_code=502, detail="Слишком много перенаправлений потока")
        if response.status_code >= 400:
            status_code = response.status_code
            await response.aclose()
            raise HTTPException(status_code=status_code, detail="Не удалось получить поток")

        upstream_status = response.status_code
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        is_m3u8 = ".m3u8" in url.lower() or "mpegurl" in content_type.lower() or "application/vnd.apple" in content_type.lower()

        if is_m3u8:
            body = await response.aread()
            await response.aclose()
            playlist = rewrite_m3u8_playlist(body.decode("utf-8", errors="replace"), url, user_agent, referer)
            return Response(
                content=playlist,
                status_code=upstream_status,
                media_type="application/vnd.apple.mpegurl",
                headers={
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "no-store",
                    "X-Proxy-Upstream-Status": str(upstream_status),
                    "X-Proxy-Upstream-Content-Type": content_type,
                },
            )

        response_headers = {
            "Content-Type": content_type,
            "Accept-Ranges": response.headers.get("Accept-Ranges", "bytes"),
            "X-Proxy-Upstream-Status": str(upstream_status),
            "X-Proxy-Upstream-Content-Type": content_type,
        }
        for header in ("Content-Length", "Content-Range"):
            if value := response.headers.get(header):
                response_headers[header] = value

        async def stream_body():
            try:
                async for chunk in response.aiter_bytes(chunk_size=512 * 1024):
                    yield chunk
            finally:
                await response.aclose()

        return StreamingResponse(stream_body(), status_code=upstream_status, headers=response_headers)
    except HTTPException:
        raise
    except httpx.HTTPError:
        raise HTTPException(status_code=502, detail="Ошибка подключения к потоку")


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await websocket.accept()
    room = get_room(room_id)
    room["connections"].add(websocket)

    server_time = time.time()
    init_state = get_current_room_state(room)
    await websocket.send_text(json.dumps({
        "type": "init",
        "state": init_state,
        "server_time": server_time,
    }))

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            msg_type = msg.get("type")

            if msg_type == "sync_request":
                server_time = time.time()
                sync_state = get_current_room_state(room)
                await websocket.send_text(json.dumps({
                    "type": "sync",
                    "state": sync_state,
                    "server_time": server_time,
                    "playing": sync_state["playing"],
                    "time": sync_state["time"],
                }))
                continue

            if msg_type == "load":
                now = time.time()
                room["state"].update({
                    "vk_url": msg.get("vk_url", ""),
                    "stream_url": msg.get("stream_url", ""),
                    "is_hls": bool(msg.get("is_hls", False)),
                    "http_headers": msg.get("http_headers") or {},
                    "time": 0.0,
                    "playing": True,
                    "updated_at": now,
                })
            elif msg_type in ("play", "pause", "seek"):
                now = time.time()
                state_time = msg.get("time", room["state"]["time"])
                try:
                    state_time = max(0.0, float(state_time))
                except (TypeError, ValueError):
                    state_time = room["state"]["time"]
                room["state"]["time"] = state_time
                room["state"]["playing"] = msg_type == "play"
                room["state"]["updated_at"] = now

            disconnected = []
            for connection in room["connections"]:
                if connection is websocket:
                    continue
                try:
                    await connection.send_text(data)
                except Exception:
                    disconnected.append(connection)
            for connection in disconnected:
                room["connections"].discard(connection)
    except (WebSocketDisconnect, json.JSONDecodeError):
        pass
    finally:
        room["connections"].discard(websocket)
        if not room["connections"]:
            rooms.pop(room_id, None)
