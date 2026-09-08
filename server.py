import os
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from telethon import TelegramClient

app = FastAPI()

# Render environment variables se values uthayenge
API_ID = int(os.getenv("API_ID", "0"))  # Apna API_ID dalein
API_HASH = os.getenv("API_HASH", "")  # Apna API_HASH dalein
BOT_TOKEN = os.getenv("BOT_TOKEN", "")  # Apna Bot Token dalein

# Private channel ki numeric ID (-100 lagakar likhna zaroori hai)
# Jaise t.me/c/4308155230/27 hai, toh ID -1004308155230 hogi
CHANNEL = -1004308155230

# Bot Token ke sath client initialize karein (Session file ki zaroorat nahi)
client = TelegramClient("bot_session", API_ID, API_HASH)


@app.on_event("startup")
async def startup_event():
  # Bot token ke sath start karein
  await client.start(bot_token=BOT_TOKEN)


@app.on_event("shutdown")
async def shutdown_event():
  await client.disconnect()


# --- UptimeRobot ke liye Root Route ---
@app.get("/")
def home():
  return {
      "status": "Active",
      "message": "Telegram Bot Streaming Proxy is Running!",
  }


# --- Video Streaming Route ---
@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request):
  try:
    message = await client.get_messages(CHANNEL, ids=message_id)
    if not message or not message.media:
      raise HTTPException(
          status_code=404, detail="Video not found or bot has no access!"
      )

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

    # 2MB ka chunk size streaming ke liye
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
