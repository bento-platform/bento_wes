from fastapi import Header, Form
from pydantic import BaseModel
from typing import Literal, Optional, Annotated

__all__ = [
    "RunStream",
]

RunStream = Literal["stdout", "stderr"]


class AuthHeaderModel(BaseModel):
    Authorization: Optional[str] = None

    def as_dict(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_header(cls, Authorization: Annotated[str | None, Header()] = None) -> "AuthHeaderModel":
        return cls(Authorization=Authorization)

    @classmethod
    def from_form(cls, token: Annotated[str, Form(...)] = None) -> "AuthHeaderModel":
        return cls(Authorization=token)
