import os
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

app = FastAPI()

# Render environment variables se values uthayega
API_ID = int(
    os.getenv("API_ID", "0")
)  # Apne Render env variables me API_ID daalna na bhulein
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
CHANNEL = "xeonmoviessite"

# Telegram Client Initialize karein
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


@app.on_event("startup")
async def startup_event():
  await client.start()


@app.on_event("shutdown")
async def shutdown_event():
  await client.disconnect()


# --- UptimeRobot ke liye Root Route (Yeh 200 OK dega) ---
@app.get("/")
def home():
  return {"status": "Active", "message": "Telegram Streaming Proxy is Running!"}


# --- Video Streaming Route ---
@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request):
  try:
    message = await client.get_messages(CHANNEL, ids=message_id)
    if not message or not message.media:
      raise HTTPException(status_code=404, detail="Video not found!")

    file_size = message.file.size
    mime_type = message.file.mime_type or "video/mp4"

    # Range headers support (video forward/backward karne ke liye)
    range_header = request.headers.get("range")
    start = 0
    end = file_size - 1

    if range_header:
      try:
        bytes_range = range_header.replace("bytes=", "").split("-")
        start = int(bytes_range[0])
        if len(bytes_range) > 1 and bytes_range[1]:
          end = int(bytes_range[1])
      except ValueError:
        pass

    # Speed behtar karne ke liye 2MB ka chunk size
    chunk_size = 1024 * 1024 * 2
    total_size = (end - start) + 1

    async def file_generator():
      async for chunk in client.iter_download(
          message.media, offset=start, limit=total_size, chunk_size=chunk_size
      ):
        yield chunk

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(total_size),
        "Content-Type": mime_type,
    }

    return StreamingResponse(
        file_generator(), status_code=206 if range_header else 200, headers=headers
    )

  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
