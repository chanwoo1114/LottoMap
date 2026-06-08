CREATE TABLE user_favorite_stores (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id)  ON DELETE CASCADE,
    store_id    INTEGER NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, store_id)
);

CREATE INDEX ix_fav_user ON user_favorite_stores (user_id);

COMMENT ON TABLE  user_favorite_stores IS '유저별 즐겨찾기(단골) 판매점';
COMMENT ON COLUMN user_favorite_stores.store_id IS '판매점 FK (stores.id)';



CREATE TABLE user_lotto_numbers (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    round_no      INTEGER NOT NULL,
    numbers       SMALLINT[] NOT NULL,
    source        VARCHAR(20) DEFAULT 'manual',
    matched_count SMALLINT,
    matched_bonus BOOLEAN,
    matched_rank  SMALLINT,
    memo          VARCHAR(100) DEFAULT '',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    scored_at     TIMESTAMPTZ
);

CREATE INDEX ix_user_numbers_user  ON user_lotto_numbers (user_id, created_at DESC);
CREATE INDEX ix_user_numbers_unscored
    ON user_lotto_numbers (round_no) WHERE matched_rank IS NULL;

COMMENT ON TABLE  user_lotto_numbers IS '유저가 저장한 로또 번호 + 당첨 대조 결과';
COMMENT ON COLUMN user_lotto_numbers.numbers IS '선택 번호 6개';
COMMENT ON COLUMN user_lotto_numbers.source IS '입력 출처 (manual/generated/qr)';
COMMENT ON COLUMN user_lotto_numbers.matched_rank IS '0=낙첨, 1~5=당첨 등수, NULL=미채점';