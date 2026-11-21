import json
from typing import Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
import yt_dlp
import httpx

app = FastAPI()

rooms: Dict[str, dict] = {}

def get_room(room_id: str):
    if room_id not in rooms:
        rooms[room_id] = {
            "connections": [],
            "state": {
                "vk_url": "",
                "stream_url": "",
                "playing": False,
                "time": 0.0
            }
        }
    return rooms[room_id]

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
            return {"stream_url": stream_url, "title": title}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/proxy")
async def proxy_video(url: str, request: Request):
    headers = {}
    if "range" in request.headers:
        headers["Range"] = request.headers["range"]
    headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

    client = httpx.AsyncClient(follow_redirects=True, timeout=30.0)
    req = client.build_request("GET", url, headers=headers)
    r = await client.send(req, stream=True)

    resp_headers = {
        "Content-Type": r.headers.get("Content-Type", "video/mp4"),
        "Accept-Ranges": "bytes",
    }
    if "Content-Length" in r.headers:
        resp_headers["Content-Length"] = r.headers["Content-Length"]
    if "Content-Range" in r.headers:
        resp_headers["Content-Range"] = r.headers["Content-Range"]

    async def iter_file():
        try:
            async for chunk in r.aiter_bytes(chunk_size=1024 * 512):
                yield chunk
        finally:
            await r.aclose()
            await client.aclose()

    return StreamingResponse(
        iter_file(),
        status_code=r.status_code,
        headers=resp_headers
    )

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await websocket.accept()
    room = get_room(room_id)
    room["connections"].append(websocket)

    await websocket.send_text(json.dumps({"type": "init", "state": room["state"]}))

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)

            if msg["type"] == "load":
                room["state"]["vk_url"] = msg["vk_url"]
                room["state"]["stream_url"] = msg["stream_url"]
                room["state"]["time"] = 0.0
                room["state"]["playing"] = True
            elif msg["type"] in ["play", "pause", "seek"]:
                room["state"]["playing"] = (msg["type"] == "play")
                room["state"]["time"] = msg.get("time", room["state"]["time"])

            for conn in room["connections"]:
                if conn != websocket:
                    await conn.send_text(data)
    except WebSocketDisconnect:
        room["connections"].remove(websocket)
