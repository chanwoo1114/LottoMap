import httpx
from fastapi import HTTPException, status
from pydantic import BaseModel

_TIMEOUT = 10


class SocialProfile(BaseModel):
    provider: str
    uid: str
    email: str | None = None
    nickname: str | None = None
    profile_image: str | None = None


async def _get(url:str, access_token: str) -> dict:
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        try:
            resp = await client.get(
                url, headers={"Authorization": f"Bearer {access_token}"}
            )
        except httpx.HTTPError:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="소셜 제공자와 통신에 실패했습니다.",
            )

    if resp.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="유효하지 않은 소셜 토큰입니다.",
        )
    if resp.status_code != status.HTTP_200_OK:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="소셜 사용자 정보 조회에 실패했습니다.",
        )
    return resp.json()

async def _kakao(access_token: str) -> SocialProfile:
    data = await _get("https://kapi.kakao.com/v2/user/me", access_token)
    account = data.get("kakao_account") or {}
    profile = account.get("profile") or {}
    return SocialProfile(
        provider="kakao",
        uid=str(data["id"]),
        email=account.get("email"),
        nickname=profile.get("nickname"),
        profile_image=profile.get("profile_image_url"),
    )


async def _naver(access_token: str) -> SocialProfile:
    data = await _get("https://openapi.naver.com/v1/nid/me", access_token)
    r = data.get("response") or {}
    return SocialProfile(
        provider="naver",
        uid=str(r["id"]),
        email=r.get("email"),
        nickname=r.get("nickname"),
        profile_image=r.get("profile_image"),
    )


async def _google(access_token: str) -> SocialProfile:
    data = await _get("https://www.googleapis.com/oauth2/v3/userinfo", access_token)
    return SocialProfile(
        provider="google",
        uid=str(data["sub"]),
        email=data.get("email"),
        nickname=data.get("name"),
        profile_image=data.get("picture"),
    )

_FETCHERS = {"kakao": _kakao, "naver": _naver, "google": _google}

async def fetch_social_profile(provider: str, access_token: str) -> SocialProfile:
    """provider에 맞는 userinfo를 조회해 SocialProfile로 반환."""
    fetcher = _FETCHERS.get(provider)
    if fetcher is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="지원하지 않는 소셜 제공자입니다.",
        )
    try:
        return await fetcher(access_token)
    except (KeyError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="소셜 사용자 정보 형식이 올바르지 않습니다.",
        )