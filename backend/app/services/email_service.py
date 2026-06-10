import aiosmtplib
from email.message import EmailMessage

from app.core.config import settings

def _build_message(to: str, subject: str, html:str, text:str) -> EmailMessage:
    """텍스트·HTML 본문을 담은 이메일 메시지를 생성."""
    msg = EmailMessage()
    msg["From"] = f"{settings.SMTP_FROM_NAME} <{settings.SMTP_USER}>"
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(text)
    msg.add_alternative(html, subtype="html")
    return msg


async def send_verification_code(to: str, code: str) -> None:
    """이메일 인증 코드를 SMTP로 발송."""
    subject = "[복권지도] 이메일 인증 코드"
    text = (
        f"인증 코드: {code}\n"
        f"{settings.EMAIL_CODE_TTL_MINUTES}분 안에 입력해주세요."
    )
    html = f"""
    <div style="font-family:'Pretendard',sans-serif;padding:24px;max-width:480px;margin:auto;">
      <h2 style="color:#222;margin:0 0 16px;">복권지도 이메일 인증</h2>
      <p style="color:#444;line-height:1.6;">아래 인증 코드를 앱에 입력해주세요.</p>
      <div style="margin:24px 0;padding:16px 24px;background:#f5f5f5;border-radius:8px;
                  font-size:32px;font-weight:700;letter-spacing:8px;text-align:center;color:#111;">
        {code}
      </div>
      <p style="color:#888;font-size:13px;">
        코드는 {settings.EMAIL_CODE_TTL_MINUTES}분 후 만료됩니다.
        본인이 요청하지 않았다면 이 메일을 무시해주세요.
      </p>
    </div>
    """
    await aiosmtplib.send(
        _build_message(to, subject, html, text),
        hostname=settings.SMTP_HOST,
        port=settings.SMTP_PORT,
        username=settings.SMTP_USER,
        password=settings.SMTP_PASSWORD,
        start_tls=True,
    )