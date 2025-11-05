import aiofiles
from datetime import datetime, timezone
from fastapi import UploadFile
from logging import Logger
from pathlib import Path
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
    logger: Logger,
    allowed_content_types: set[str] | None = None,
    chunk_size: int = CHUNK_SIZE,
    max_bytes_per_file: int | None = None,
    overwrite: bool = False,
) -> list[UploadFileResult]:
    """
    Streams each UploadFile to disk (non-blocking) with basic safety checks.

    Returns a list of per-file results:
      success: { "filename": ..., "path": ..., "content_type": ..., "size": ... } (type: UploadFileSuccess)
      error:   { "filename": ..., "error": ..., ["content_type": ...] }           (type: uploadFileError)
    """

    dest_dir.mkdir(parents=True, exist_ok=True)

    results: list[UploadFileResult] = []

    for f in files:
        safe_name = Path(f.filename or "unnamed").name  # sanitize
        dest = dest_dir / safe_name

        # content-type check
        if allowed_content_types and f.content_type not in allowed_content_types:
            results.append(UploadFileError(filename=safe_name, error=f"Unsupported content type: {f.content_type}"))
            await f.close()
            continue

        # unique name if not overwriting
        if not overwrite and dest.exists():
            stem, suffix = dest.stem, dest.suffix
            i = 1
            while dest.exists():
                dest = dest_dir / f"{stem} ({i}){suffix}"
                i += 1

        size: int = 0
        error: str | None = None

        try:
            async with aiofiles.open(dest, "wb") as out:
                while chunk := await f.read(chunk_size):
                    size += len(chunk)
                    if max_bytes_per_file is not None and size > max_bytes_per_file:
                        error = f"File exceeds max size ({max_bytes_per_file} bytes)"
                        break
                    await out.write(chunk)
        except Exception as e:  # e.g., disk errors
            logger.exception("encountered error while saving file upload", exc_info=e)
            error = f"I/O error: {e}"
        finally:
            await f.close()

        # if size limit tripped, remove partial file
        if error and dest.exists():
            try:
                dest.unlink(missing_ok=True)
            except Exception as e:
                logger.exception("encountered error while removing partial file upload", exc_info=e)

        results.append(
            UploadFileError(filename=safe_name, content_type=f.content_type, error=error)
            if error
            else UploadFileSuccess(filename=safe_name, path=str(dest), content_type=f.content_type, size=size)
        )

    return results
