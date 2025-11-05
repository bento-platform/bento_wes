import aiofiles
from datetime import datetime, timezone
from fastapi import UploadFile
from pathlib import Path
from typing import Any, Iterable


__all__ = ["iso_now", "save_upload_files"]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO date format


CHUNK_SIZE = 1024 * 1024  # 1 MiB


async def save_upload_files(
    files: Iterable[UploadFile],
    dest_dir: Path | str,
    allowed_content_types: set[str] | None = None,
    chunk_size: int = CHUNK_SIZE,
    max_bytes_per_file: int | None = None,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    """
    Streams each UploadFile to disk (non-blocking) with basic safety checks.

    Returns a list of per-file results:
      { "filename": ..., "path": ..., "content_type": ..., "size": ..., "error": ... }
    """

    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []

    for f in files:
        safe_name = Path(f.filename or "unnamed").name  # sanitize
        dest = dest_dir / safe_name

        # content-type check
        if allowed_content_types and f.content_type not in allowed_content_types:
            results.append(
                {
                    "filename": safe_name,
                    "error": f"Unsupported content type: {f.content_type}",
                }
            )
            await f.close()
            continue

        # unique name if not overwriting
        if not overwrite and dest.exists():
            stem, suffix = dest.stem, dest.suffix
            i = 1
            while dest.exists():
                dest = dest_dir / f"{stem} ({i}){suffix}"
                i += 1

        size = 0
        error: str | None = None

        try:
            async with aiofiles.open(dest, "wb") as out:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    size += len(chunk)
                    if max_bytes_per_file is not None and size > max_bytes_per_file:
                        error = f"File exceeds max size ({max_bytes_per_file} bytes)"
                        break
                    await out.write(chunk)
        except Exception as e:  # e.g., disk errors
            error = f"I/O error: {e}"
        finally:
            await f.close()

        # if size limit tripped, remove partial file
        if error and dest.exists():
            try:
                dest.unlink(missing_ok=True)
            except Exception:
                pass

        results.append(
            {
                "filename": safe_name,
                "path": None if error else str(dest),
                "content_type": f.content_type,
                "size": None if error else size,
                "error": error,
            }
        )

    return results
