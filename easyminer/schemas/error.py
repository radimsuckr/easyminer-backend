from typing import Any

from fastapi import HTTPException
from fastapi.responses import JSONResponse


class StructuredHTTPException(HTTPException):
    def __init__(
        self,
        status_code: int,
        error: str,
        message: str,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.error = error
        self.message = message
        self.details = details
        # Store detail for FastAPI compatibility
        super().__init__(status_code=status_code, detail=message, headers=headers)


def structured_error_response(
    status_code: int, error: str, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    content: dict[str, str | dict[str, Any]] = {"error": error, "message": message}
    if details:
        content["details"] = details
    return JSONResponse(status_code=status_code, content=content)
