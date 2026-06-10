from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.security import decode_access_token

bearer = HTTPBearer(auto_error=True)

async def get_current_user_id(
    cred: HTTPAuthorizationCredentials = Depends(bearer),
) -> int:
    try:
        return decode_access_token(cred.credentials)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )