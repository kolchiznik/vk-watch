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


def get_room(room_id: str) -> dict:
    if room_id not in rooms:
        rooms[room_id] = {
            "connections": set(),
            "host_token": None,
            "state": {
                "vk_url": "",
                "stream_url": "",
                "is_hls": False,
                "http_headers": {},
                "playing": False,
                "anchor_time": 0.0,
                "anchor_timestamp": time.time(),
            },
        }
    return rooms[room_id]


def get_room_state(room: dict) -> dict:
    now = time.time()
    state = room["state"]
    anchor_time = float(state["anchor_time"])
    actual_time = anchor_time
    if state["playing"]:
        actual_time += max(0.0, now - float(state["anchor_timestamp"]))
    return {
        "vk_url": state["vk_url"],
        "stream_url": state["stream_url"],
        "is_hls": state["is_hls"],
        "http_headers": state["http_headers"],
        "playing": bool(state["playing"]),
        "anchor_time": anchor_time,
        "anchor_timestamp": float(state["anchor_timestamp"]),
        "actual_time": actual_time,
        "time": actual_time,
    }


def validate_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise HTTPException(status_code=400, detail="Некорректный URL потока")
    try:
        addresses = {entry[4][0] for entry in socket.getaddrinfo(parsed.hostname, None)}
    except (socket.gaierror, ValueError):
        raise HTTPException(status_code=400, detail="Не удалось определить хост потока")
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise HTTPException(status_code=400, detail="Прокси-ссылка недоступна")


def proxy_url(url: str, user_agent: str, referer: str) -> str:
    return "/api/proxy?" + urlencode({"url": url, "user_agent": user_agent, "referer": referer})


def is_media_segment(url: str) -> bool:
    return urlparse(url).path.lower().endswith((".ts", ".m4s", ".mp4", ".aac", ".vtt"))


def rewrite_m3u8_playlist(text: str, playlist_url: str, user_agent: str, referer: str) -> str:
    """Proxy child playlists and keys, but leave media segment URLs CDN-direct."""
    import re

    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            lines.append(line)
        elif stripped.startswith("#"):
            def replace_uri(match):
                source_uri = match.group(1)
                if source_uri.lower().startswith("data:"):
                    return match.group(0)
                absolute_uri = urljoin(playlist_url, source_uri)
                target = absolute_uri if is_media_segment(absolute_uri) else proxy_url(absolute_uri, user_agent, referer)
                return f'URI="{target}"'

            lines.append(re.sub(r'URI="([^"]+)"', replace_uri, stripped, flags=re.IGNORECASE))
        else:
            absolute_uri = urljoin(playlist_url, stripped)
            if urlparse(absolute_uri).path.lower().endswith(".m3u8"):
                lines.append(proxy_url(absolute_uri, user_agent, referer))
            else:
                lines.append(absolute_uri)
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


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
        extracted = info.get("http_headers") or {}
        headers = {
            "User-Agent": extracted.get("User-Agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": extracted.get("Referer") or "https://vkvideo.ru/",
        }
        validate_public_url(stream_url)
        return {
            "stream_url": stream_url,
            "title": info.get("title", "Видео ВК"),
            "is_hls": is_hls,
            "ext": ext,
            "protocol": protocol,
            "http_headers": headers,
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
            return Response(content=playlist, status_code=upstream_status, media_type="application/vnd.apple.mpegurl", headers={"Cache-Control": "no-store"})

        response_headers = {
            "Content-Type": content_type,
            "Accept-Ranges": response.headers.get("Accept-Ranges", "bytes"),
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

    # The first client frame is auth; claim the token once and retain it with room state.
    try:
        auth_message = json.loads(await websocket.receive_text())
    except (WebSocketDisconnect, json.JSONDecodeError):
        return
    supplied_token = auth_message.get("host_token") if auth_message.get("type") == "auth" else None
    if room["host_token"] is None and isinstance(supplied_token, str) and supplied_token:
        room["host_token"] = supplied_token
    is_operator = bool(supplied_token and supplied_token == room["host_token"])
    room["connections"].add(websocket)

    now = time.time()
    await websocket.send_text(json.dumps({
        "type": "room_state",
        "state": get_room_state(room),
        "server_time": now,
        "role": "operator" if is_operator else "viewer",
    }))

    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
            except json.JSONDecodeError:
                continue
            message_type = message.get("type")
            if message_type == "sync_request":
                await websocket.send_text(json.dumps({
                    "type": "room_state",
                    "state": get_room_state(room),
                    "server_time": time.time(),
                }))
                continue
            if message_type not in ("load", "play", "pause", "seek") or not is_operator:
                continue

            state = room["state"]
            now = time.time()
            if message_type == "load":
                state.update({
                    "vk_url": message.get("vk_url", ""),
                    "stream_url": message.get("stream_url", ""),
                    "is_hls": bool(message.get("is_hls", False)),
                    "http_headers": message.get("http_headers") or {},
                    "anchor_time": 0.0,
                    "playing": True,
                    "anchor_timestamp": now,
                })
            else:
                try:
                    current_time = max(0.0, float(message.get("time", state["anchor_time"])))
                except (TypeError, ValueError):
                    current_time = float(state["anchor_time"])
                state["anchor_time"] = current_time
                state["anchor_timestamp"] = now
                if message_type == "play":
                    state["playing"] = True
                elif message_type == "pause":
                    state["playing"] = False
                # seek preserves the current playing state

            event = {
                "type": "room_state",
                "state": get_room_state(room),
                "server_time": time.time(),
            }
            disconnected = []
            for connection in tuple(room["connections"]):
                try:
                    await connection.send_text(json.dumps(event))
                except Exception:
                    disconnected.append(connection)
            for connection in disconnected:
                room["connections"].discard(connection)
    except WebSocketDisconnect:
        pass
    finally:
        room["connections"].discard(websocket)
