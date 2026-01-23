import aiofiles
from datetime import datetime, timezone
from fastapi import UploadFile
from pathlib import Path
from structlog.stdlib import BoundLogger
from typing import Iterable, NotRequired, TypedDict


__all__ = [
    "UploadFileError",
    "UploadFileSuccess",
    "UploadFileResult",
    "iso_now",
    "save_upload_files",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


CHUNK_SIZE = 1024 * 1024  # 1 MiB


class UploadFileError(TypedDict):
    filename: str
    error: str
    content_type: NotRequired[str | None]


class UploadFileSuccess(TypedDict):
    filename: str
    path: str
    content_type: str | None
    size: int


UploadFileResult = UploadFileError | UploadFileSuccess


async def save_upload_files(
    files: Iterable[UploadFile],
    dest_dir: Path,
    logger: BoundLogger,
) -> list[UploadFileResult]:
    """
    Streams each UploadFile to disk (non-blocking).

    Returns a list of per-file results:
      success: { "filename": ..., "path": ..., "content_type": ..., "size": ... } (type: UploadFileSuccess)
      error:   { "filename": ..., "error": ..., ["content_type": ...] }           (type: UploadFileError)
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    results: list[UploadFileResult] = []

    for f in files:
        safe_name = Path(f.filename or "unnamed").name  # sanitize path traversal
        dest = dest_dir / safe_name

        # Generate unique name if file exists
        if dest.exists():
            stem, suffix = dest.stem, dest.suffix
            i = 1
            while dest.exists():
                dest = dest_dir / f"{stem} ({i}){suffix}"
                i += 1

        size: int = 0
        error: str | None = None

        try:
            async with aiofiles.open(dest, "wb") as out:
                while chunk := await f.read(CHUNK_SIZE):
                    size += len(chunk)
                    await out.write(chunk)
        except Exception as e:
            await logger.aexception("encountered error while saving file upload", exc_info=e)
            error = f"I/O error: {e}"
        finally:
            await f.close()

        # If error occurred, remove partial file
        if error and dest.exists():
            try:
                dest.unlink(missing_ok=True)
            except Exception as e:
                await logger.aexception("encountered error while removing partial file upload", exc_info=e)

        results.append(
            UploadFileError(filename=safe_name, content_type=f.content_type, error=error)
            if error
            else UploadFileSuccess(filename=safe_name, path=str(dest), content_type=f.content_type, size=size)
        )

    return results
