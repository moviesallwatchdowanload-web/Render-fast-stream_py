import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession
import uvicorn

app = FastAPI()

API_ID = 38488665
API_HASH = '9ad7d69c232114cb80f6c0f666b2201d'
CHANNEL_USERNAME = '@xeonmoviessite'

SESSION_STRING = os.environ.get("SESSION_STRING", "")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@app.on_event("startup")
async def startup_event():
    await client.start()
    print("Telethon Stream Client Started successfully!")

@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request):
    try:
        message = await client.get_messages(CHANNEL_USERNAME, ids=message_id)
        
        if not message or not message.media:
            raise HTTPException(status_code=404, detail="Video not found!")

        file_size = message.file.size
        mime_type = message.file.mime_type or 'video/mp4'

        range_header = request.headers.get("range")
        start = 0
        end = file_size - 1

        if range_header:
            byte_range = range_header.replace("bytes=", "").split("-")
            start = int(byte_range[0])
            if byte_range[1]:
                end = int(byte_range[1])

        chunk_size = 1024 * 1024
        length = (end - start) + 1

        async def video_generator():
            async for chunk in client.iter_download(message.media, offset=start, chunk_size=chunk_size, limit=length):
                yield chunk

        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Type": mime_type,
        }

        return StreamingResponse(video_generator(), status_code=206 if range_header else 200, headers=headers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

