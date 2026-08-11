from typing import Annotated, Literal, Self

from fastapi import Form, Header
from pydantic import BaseModel

__all__ = [
    "RunStream",
    "AuthHeaderModel",
]

RunStream = Literal["stdout", "stderr"]


class AuthHeaderModel(BaseModel):
    Authorization: str | None = None

    def as_dict(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_header(cls, authorization: Annotated[str | None, Header()] = None) -> Self:
        return cls(Authorization=authorization)

    @classmethod
    def from_form(cls, token: Annotated[str, Form(...)] = "") -> Self:
        return cls(Authorization=f"Bearer {token}")
