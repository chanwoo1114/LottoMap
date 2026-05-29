 CREATE TABLE users (
  id            SERIAL PRIMARY KEY,
  email         VARCHAR(255) UNIQUE,
  password VARCHAR(255),
  nickname      VARCHAR(50) NOT NULL,
  profile_image TEXT DEFAULT '',
  is_active     BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER trg_user_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE  users IS '서비스 회원';
COMMENT ON COLUMN users.email IS '이메일';
COMMENT ON COLUMN users.password IS '비밀번호 해시';
COMMENT ON COLUMN users.nickname IS '닉네임';
COMMENT ON COLUMN users.profile_image IS '프로필 이미지 URL';
COMMENT ON COLUMN users.is_active IS '활성 여부';


CREATE TABLE user_oauth (
  id           SERIAL PRIMARY KEY,
  user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider     VARCHAR(20) NOT NULL,
  provider_uid VARCHAR(100) NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (provider, provider_uid)
);

  CREATE INDEX ix_user_oauth_user ON user_oauth (user_id);

COMMENT ON TABLE  user_oauth IS '소셜 로그인 연결';
COMMENT ON COLUMN user_oauth.provider IS '소셜 제공자 (kako, naver, google)';
COMMENT ON COLUMN user_oauth.provider_uid IS '카카오 회원번호 ID';


CREATE TABLE refresh_tokens (
  id         SERIAL PRIMARY KEY,
  user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash VARCHAR(64) UNIQUE NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX ix_refresh_tokens_user ON refresh_tokens (user_id);

COMMENT ON TABLE  refresh_tokens IS '리프레시 토큰';
COMMENT ON COLUMN refresh_tokens.token_hash IS '리프레시 토큰 해시';
COMMENT ON COLUMN refresh_tokens.revoked_at IS '무효화 시각';


CREATE TABLE email_verifications (
  id          SERIAL PRIMARY KEY,
  email       VARCHAR(255) NOT NULL,
  code_hash   VARCHAR(64)  NOT NULL,
  expires_at  TIMESTAMPTZ  NOT NULL,
  verified_at TIMESTAMPTZ,
  attempts    SMALLINT     DEFAULT 0,
  created_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX ix_email_verif_recent
    ON email_verifications (email, created_at DESC);

COMMENT ON TABLE  email_verifications IS '회원가입 이메일 인증 코드';
COMMENT ON COLUMN email_verifications.code_hash IS '6자리 코드 해시';
COMMENT ON COLUMN email_verifications.expires_at IS '코드 만료 시각';
COMMENT ON COLUMN email_verifications.verified_at IS '검증 성공 시각';
COMMENT ON COLUMN email_verifications.attempts IS '검증 시도 횟수';