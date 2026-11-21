import json
import ipaddress
import socket
from urllib.parse import urlparse
from typing import Dict, Set

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI()

rooms: Dict[str, dict] = {}


def get_room(room_id: str):
    if room_id not in rooms:
        rooms[room_id] = {
            "connections": set(),
            "state": {
                "vk_url": "",
                "stream_url": "",
                "playing": False,
                "time": 0.0,
            },
        }
    return rooms[room_id]


def validate_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise HTTPException(status_code=400, detail="Некорректный URL потока")

    hostname = parsed.hostname
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
            validate_public_url(stream_url)
            return {"stream_url": stream_url, "title": title}
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
            redirect_url = str(response.next_request.url) if response.next_request else location
            validate_public_url(redirect_url)
            response = await client.send(
                client.build_request("GET", redirect_url, headers=headers),
                stream=True,
            )

        if response.status_code >= 400:
            await response.aclose()
            await client.aclose()
            raise HTTPException(status_code=response.status_code, detail="Не удалось получить поток")

        response_headers = {
            "Content-Type": response.headers.get("Content-Type", "video/mp4"),
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

            if not room["connections"]:
                rooms.pop(room_id, None)
    except WebSocketDisconnect:
        pass
    except json.JSONDecodeError:
        pass
    finally:
        room["connections"].discard(websocket)
        if not room["connections"]:
            rooms.pop(room_id, None)
