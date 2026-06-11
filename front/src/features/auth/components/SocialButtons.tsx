import { FcGoogle } from 'react-icons/fc';
import { RiKakaoTalkFill } from 'react-icons/ri';
import { SiNaver } from 'react-icons/si';
import { useSocialLogin } from '../hooks/useSocialLogin';

interface SocialButtonsProps {
  onClose: () => void;
}

export function SocialButtons({ onClose }: SocialButtonsProps) {
  const { login, loading, error } = useSocialLogin(onClose);
  const busy = loading !== null;

  return (
    <div className="flex flex-col gap-3">
      {/* 구분선 */}
      <div className="flex items-center gap-3 text-xs text-gray-400">
        <span className="h-px flex-1 bg-gray-200" />
        또는
        <span className="h-px flex-1 bg-gray-200" />
      </div>

      <div className="flex justify-center gap-4">
        {/* 카카오 */}
        <button
          type="button"
          onClick={() => login('kakao')}
          disabled={busy}
          aria-label="카카오로 시작하기"
          className="flex h-12 w-12 items-center justify-center rounded-full bg-[#FEE500] transition hover:opacity-90 disabled:opacity-50"
        >
          <RiKakaoTalkFill className="h-6 w-6 text-[#3C1E1E]" />
        </button>

        {/* 네이버 */}
        <button
          type="button"
          onClick={() => login('naver')}
          disabled={busy}
          aria-label="네이버로 시작하기"
          className="flex h-12 w-12 items-center justify-center rounded-full bg-[#03C75A] transition hover:opacity-90 disabled:opacity-50"
        >
          <SiNaver className="h-4 w-4 text-white" />
        </button>

        {/* 구글 */}
        <button
          type="button"
          onClick={() => login('google')}
          disabled={busy}
          aria-label="구글로 시작하기"
          className="flex h-12 w-12 items-center justify-center rounded-full border border-gray-200 bg-white transition hover:bg-gray-50 disabled:opacity-50"
        >
          <FcGoogle className="h-6 w-6" />
        </button>
      </div>

      {error && <p className="text-center text-sm text-red-500">{error}</p>}
    </div>
  );
}
