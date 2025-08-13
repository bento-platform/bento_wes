from fastapi import Header
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
    def from_header(
        cls,
        Authorization: Annotated[str | None, Header()] = None
    ) -> "AuthHeaderModel":
        return cls(Authorization=Authorization)