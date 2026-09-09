import os
import re
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from telethon import TelegramClient

app = FastAPI()

# =========================
# CONFIG
# =========================

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

CHANNEL = -1004308155230

# Telethon's practical MTProto request size
REQUEST_SIZE = 512 * 1024

client = TelegramClient(
    "bot_session",
    API_ID,
    API_HASH,
    connection_retries=None,
    retry_delay=1,
    auto_reconnect=True,
)

# =========================
# START / STOP
# =========================

@app.on_event("startup")
async def startup_event():
    await client.start(bot_token=BOT_TOKEN)


@app.on_event("shutdown")
async def shutdown_event():
    await client.disconnect()


# =========================
# HEALTH CHECK
# =========================

@app.get("/")
async def home():
    return {
        "status": "Active",
        "stream": "Telegram → FastAPI",
        "range": "enabled"
    }


# =========================
# RANGE PARSER
# =========================

def parse_range(range_header: str | None, file_size: int):

    if not range_header:
        return 0, file_size - 1, False

    match = re.fullmatch(
        r"bytes=(\d*)-(\d*)",
        range_header.strip()
    )

    if not match:
        raise HTTPException(
            status_code=416,
            headers={
                "Content-Range": f"bytes */{file_size}"
            }
        )

    start_str, end_str = match.groups()

    # bytes=-500000
    if not start_str:
        length = int(end_str)

        if length <= 0:
            raise HTTPException(
                status_code=416,
                headers={
                    "Content-Range": f"bytes */{file_size}"
                }
            )

        start = max(file_size - length, 0)
        end = file_size - 1

    else:
        start = int(start_str)

        if start >= file_size:
            raise HTTPException(
                status_code=416,
                headers={
                    "Content-Range": f"bytes */{file_size}"
                }
            )

        if end_str:
            end = min(int(end_str), file_size - 1)
        else:
            end = file_size - 1

    if start > end:
        raise HTTPException(
            status_code=416,
            headers={
                "Content-Range": f"bytes */{file_size}"
            }
        )

    return start, end, True


# =========================
# VIDEO STREAM
# =========================

@app.get("/stream/{message_id}")
async def stream_video(
    message_id: int,
    request: Request
):

    try:
        # -------------------------
        # GET TELEGRAM MESSAGE
        # -------------------------

        message = await client.get_messages(
            CHANNEL,
            ids=message_id
        )

        if not message or not message.media:
            raise HTTPException(
                status_code=404,
                detail="Video not found"
            )

        if not message.file:
            raise HTTPException(
                status_code=404,
                detail="Media is not a file"
            )

        file_size = message.file.size

        if not file_size:
            raise HTTPException(
                status_code=404,
                detail="File size unavailable"
            )

        mime_type = (
            message.file.mime_type
            or "video/mp4"
        )

        # -------------------------
        # RANGE / SEEK
        # -------------------------

        range_header = request.headers.get("range")

        start, end, is_range = parse_range(
            range_header,
            file_size
        )

        content_length = end - start + 1

        # -------------------------
        # STREAM GENERATOR
        # -------------------------

        async def file_generator():

            try:

                async for chunk in client.iter_download(
                    message.media,
                    offset=start,
                    limit=content_length,
                    request_size=REQUEST_SIZE,
                    chunk_size=REQUEST_SIZE,
                ):

                    if await request.is_disconnected():
                        return

                    yield chunk

            except Exception as error:

                print(
                    f"[STREAM ERROR] "
                    f"message={message_id} "
                    f"offset={start} "
                    f"error={error}"
                )

        # -------------------------
        # RESPONSE HEADERS
        # -------------------------

        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Type": mime_type,

            # Browser caching hint
            "Cache-Control": "public, max-age=3600",

            # CORS
            "Access-Control-Allow-Origin": "*",

            # Helps keep connection alive
            "Connection": "keep-alive",

            # Debug
            "X-Stream-Source": "telegram"
        }

        if is_range:

            headers["Content-Range"] = (
                f"bytes {start}-{end}/{file_size}"
            )

            status_code = 206

        else:

            status_code = 200

        return StreamingResponse(
            file_generator(),
            status_code=status_code,
            headers=headers,
            media_type=mime_type
        )

    except HTTPException:
        raise

    except Exception as error:

        print(
            f"[REQUEST ERROR] "
            f"message={message_id} "
            f"error={error}"
        )

        raise HTTPException(
            status_code=500,
            detail="Streaming error"
        )
