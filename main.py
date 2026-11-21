import json
import ipaddress
import socket
from urllib.parse import urljoin, urlparse, quote
from typing import Dict

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse, Response

app = FastAPI()

rooms: Dict[str, dict] = {}

VK_CDN_SUFFIXES = (
    "vkuservideo.net",
    "mycdn.me",
    "okcdn.ru",
    "vk.com",
    "vkvideo.ru",
)


def get_room(room_id: str):
    if room_id not in rooms:
        rooms[room_id] = {
            "connections": set(),
            "state": {
                "vk_url": "",
                "stream_url": "",
                "is_hls": False,
                "playing": False,
                "time": 0.0,
            },
        }
    return rooms[room_id]


def is_vk_cdn_hostname(hostname: str) -> bool:
    hostname = hostname.lower().rstrip(".")
    return any(hostname == suffix or hostname.endswith("." + suffix) for suffix in VK_CDN_SUFFIXES)


def validate_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise HTTPException(status_code=400, detail="Некорректный URL потока")

    hostname = parsed.hostname.lower().rstrip(".")

    if is_vk_cdn_hostname(hostname):
        return

    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(hostname, None)}
    except socket.gaierror:
        raise HTTPException(status_code=400, detail="Не удалось определить хост потока")

    for address in addresses:
        ip = ipaddress.ip_address(address)
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise HTTPException(status_code=400, detail="Прокси-ссылка недоступна")


def rewrite_m3u8_playlist(text: str, playlist_url: str) -> str:
    rewritten_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            rewritten_lines.append(line)
            continue

        if stripped.startswith("#"):
            if stripped.upper().startswith("#EXT-X-KEY:") or stripped.upper().startswith("#EXT-X-MEDIA:"):
                prefix, attributes = stripped.split(":", 1)
                import re

                def replace_uri(match):
                    uri = match.group(2)
                    if uri.startswith("data:"):
                        return match.group(0)
                    absolute_uri = urljoin(playlist_url, uri.strip('"'))
                    return f'{match.group(1)}="/api/proxy?url={quote(absolute_uri, safe="")}"'

                stripped = re.sub(r'(URI=(?:"([^"]+)"|([^,]+)))',
                                  lambda m: (f'URI="/api/proxy?url={quote(urljoin(playlist_url, (m.group(2) or m.group(3)).strip(chr(34))), safe="")}"'),
                                  stripped,
                                  flags=re.IGNORECASE)
            rewritten_lines.append(stripped)
            continue

        absolute_uri = urljoin(playlist_url, stripped)
        rewritten_lines.append(f'/api/proxy?url={quote(absolute_uri, safe="")}')

    return "\n".join(rewritten_lines) + ("\n" if text.endswith("\n") else "")


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()


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
            title = info.get("title", "Видео ВК")
            if not stream_url:
                raise ValueError("Не удалось получить поток")

            ext = str(info.get("ext", "")).lower()
            protocol = str(info.get("protocol", "")).lower()
            is_hls = ext == "m3u8" or "m3u8" in protocol or ".m3u8" in stream_url.lower()

            validate_public_url(stream_url)
            return {"stream_url": stream_url, "title": title, "is_hls": is_hls}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/proxy")
async def proxy_video(url: str, request: Request):
    validate_public_url(url)

    headers = {}
    if range_header := request.headers.get("range"):
        headers["Range"] = range_header
    headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

    client = httpx.AsyncClient(follow_redirects=False, timeout=30.0)
    try:
        response = await client.send(
            client.build_request("GET", url, headers=headers),
            stream=True,
        )

        if response.is_redirect:
            location = response.headers.get("location")
            await response.aclose()
            if not location:
                await client.aclose()
                raise HTTPException(status_code=502, detail="Поток перенаправляет без адреса")

            redirect_url = urljoin(str(response.request.url), location)
            validate_public_url(redirect_url)
            response = await client.send(
                client.build_request("GET", redirect_url, headers=headers),
                stream=True,
            )
            url = redirect_url

        if response.status_code >= 400:
            await response.aclose()
            await client.aclose()
            raise HTTPException(status_code=response.status_code, detail="Не удалось получить поток")

        content_type = response.headers.get("Content-Type", "video/mp4")
        is_m3u8 = ".m3u8" in url.lower() or "mpegurl" in content_type.lower() or "application/vnd.apple" in content_type.lower()

        if is_m3u8:
            text = await response.aread()
            await response.aclose()
            await client.aclose()
            rewritten = rewrite_m3u8_playlist(text.decode("utf-8", errors="replace"), url)
            return Response(
                content=rewritten,
                status_code=response.status_code,
                media_type="application/vnd.apple.mpegurl",
                headers={"Accept-Ranges": "bytes", "Cache-Control": "no-store"},
            )

        response_headers = {
            "Content-Type": content_type,
            "Accept-Ranges": "bytes",
        }
        if content_length := response.headers.get("Content-Length"):
            response_headers["Content-Length"] = content_length
        if content_range := response.headers.get("Content-Range"):
            response_headers["Content-Range"] = content_range

        async def iter_file():
            try:
                async for chunk in response.aiter_bytes(chunk_size=1024 * 512):
                    yield chunk
            finally:
                await response.aclose()
                await client.aclose()

        return StreamingResponse(
            iter_file(),
            status_code=response.status_code,
            headers=response_headers,
        )
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
                room["state"]["vk_url"] = msg.get("vk_url", "")
                room["state"]["stream_url"] = msg.get("stream_url", "")
                room["state"]["is_hls"] = bool(msg.get("is_hls", False))
                room["state"]["time"] = 0.0
                room["state"]["playing"] = True
            elif msg.get("type") in ("play", "pause", "seek"):
                room["state"]["playing"] = msg["type"] == "play"
                room["state"]["time"] = msg.get("time", room["state"]["time"])

            disconnected = []
            for conn in room["connections"]:
                if conn == websocket:
                    continue
                try:
                    await conn.send_text(data)
                except Exception:
                    disconnected.append(conn)

            for conn in disconnected:
                room["connections"].discard(conn)

    except WebSocketDisconnect:
        pass
    except json.JSONDecodeError:
        pass
    finally:
        room["connections"].discard(websocket)
        if not room["connections"]:
            rooms.pop(room_id, None)
