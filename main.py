import ipaddress
import json
import socket
from typing import Dict
from urllib.parse import quote, urlencode, urljoin, urlparse

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response, StreamingResponse

app = FastAPI()
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
            },
        }
    return rooms[room_id]


def validate_public_url(url: str) -> None:
    """Allow public CDN hosts while rejecting loopback/private/link-local destinations."""
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
        ip = ipaddress.ip_address(address)
        if not ip.is_global:
            raise HTTPException(status_code=400, detail="Прокси-ссылка недоступна")


def proxy_url(url: str, user_agent: str, referer: str) -> str:
    return "/api/proxy?" + urlencode({
        "url": url,
        "user_agent": user_agent,
        "referer": referer,
    })


def rewrite_m3u8_playlist(text: str, playlist_url: str, user_agent: str, referer: str) -> str:
    import re

    rewritten_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            rewritten_lines.append(line)
            continue

        if stripped.startswith("#"):
            # HLS URI attributes can contain keys, alternate audio/subtitles, and I-frame playlists.
            def replace_uri(match):
                original_uri = match.group(2)
                if original_uri.lower().startswith("data:"):
                    return match.group(0)
                absolute_uri = urljoin(playlist_url, original_uri)
                return f'URI="{proxy_url(absolute_uri, user_agent, referer)}"'

            rewritten_lines.append(
                re.sub(r'URI="([^"]+)"', replace_uri, stripped, flags=re.IGNORECASE)
            )
            continue

        absolute_uri = urljoin(playlist_url, stripped)
        rewritten_lines.append(proxy_url(absolute_uri, user_agent, referer))

    return "\n".join(rewritten_lines) + ("\n" if text.endswith("\n") else "")


def stream_request_headers(user_agent: str, referer: str, request: Request) -> dict:
    headers = {
        "User-Agent": user_agent[:512],
        "Referer": referer[:2048],
    }
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
        "format": "best[ext=mp4]/best",
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

        # Keep only the headers needed to access the media CDN; never expose cookies/tokens.
        extracted_headers = info.get("http_headers") or {}
        user_agent = extracted_headers.get("User-Agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        referer = extracted_headers.get("Referer") or "https://vkvideo.ru/"
        proxy_headers = {"User-Agent": user_agent, "Referer": referer}

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
    client = httpx.AsyncClient(follow_redirects=False, timeout=30.0)

    try:
        upstream_request = client.build_request("GET", url, headers=headers)
        response = await client.send(upstream_request, stream=True)

        # Follow a small number of redirects manually so every destination is SSRF-checked.
        for _ in range(5):
            if not response.is_redirect:
                break
            location = response.headers.get("location")
            redirect_base = str(response.request.url)
            await response.aclose()
            if not location:
                raise HTTPException(status_code=502, detail="Поток перенаправляет без адреса")
            url = urljoin(redirect_base, location)
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
        is_m3u8 = (
            ".m3u8" in url.lower()
            or "mpegurl" in content_type.lower()
            or "application/vnd.apple" in content_type.lower()
        )

        if is_m3u8:
            body = await response.aread()
            await response.aclose()
            rewritten = rewrite_m3u8_playlist(
                body.decode("utf-8", errors="replace"), url, user_agent, referer
            )
            await client.aclose()
            return Response(
                content=rewritten,
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
                await client.aclose()

        return StreamingResponse(stream_body(), status_code=upstream_status, headers=response_headers)
    except HTTPException:
        await client.aclose()
        raise
    except httpx.HTTPError:
        await client.aclose()
        raise HTTPException(status_code=502, detail="Ошибка подключения к потоку")


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await websocket.accept()
    room = get_room(room_id)
    room["connections"].add(websocket)
    await websocket.send_text(json.dumps({"type": "init", "state": room["state"]}))

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)

            if msg.get("type") == "load":
                room["state"].update({
                    "vk_url": msg.get("vk_url", ""),
                    "stream_url": msg.get("stream_url", ""),
                    "is_hls": bool(msg.get("is_hls", False)),
                    "http_headers": msg.get("http_headers") or {},
                    "time": 0.0,
                    "playing": True,
                })
            elif msg.get("type") in ("play", "pause", "seek"):
                room["state"]["playing"] = msg["type"] == "play"
                room["state"]["time"] = msg.get("time", room["state"]["time"])

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
