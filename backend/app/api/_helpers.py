from fastapi import HTTPException


def or_404(result, detail: str):
    """result가 비어있으면(404) 예외, 아니면 그대로 반환."""
    if not result:
        raise HTTPException(404, detail)
    return result
