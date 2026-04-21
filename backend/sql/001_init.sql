CREATE EXTENSION IF NOT EXISTS postgis;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TABLE stores (
    id                  SERIAL PRIMARY KEY,
    store_id            VARCHAR(20) UNIQUE NOT NULL,
    name                VARCHAR(100) NOT NULL,
    address             VARCHAR(255) NOT NULL,
    address_detail      VARCHAR(255) DEFAULT '',
    phone               VARCHAR(20) DEFAULT '',
    location            GEOMETRY(POINT, 4326),
    sido                VARCHAR(20) DEFAULT '',
    sigungu             VARCHAR(20) DEFAULT '',
    dong                VARCHAR(20) DEFAULT '',
    sells_lotto         BOOLEAN DEFAULT TRUE,
    sells_pension       BOOLEAN DEFAULT FALSE,
    sells_speetto_2000  BOOLEAN DEFAULT FALSE,
    sells_speetto_1000  BOOLEAN DEFAULT FALSE,
    sells_speetto_500   BOOLEAN DEFAULT FALSE,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX ix_stores_location ON stores USING GIST (location);
CREATE INDEX ix_stores_sido_sigungu ON stores (sido, sigungu);
CREATE INDEX ix_stores_active ON stores (is_active) WHERE is_active = TRUE;

CREATE TRIGGER trg_stores_updated_at
    BEFORE UPDATE ON stores
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE stores IS '전국 복권 판매점 정보';
COMMENT ON COLUMN stores.id IS '판매점 고유 ID (PK)';
COMMENT ON COLUMN stores.store_id IS '동행복권 기준 판매점 고유번호';
COMMENT ON COLUMN stores.name IS '판매점 상호명';
COMMENT ON COLUMN stores.address IS '판매점 주소';
COMMENT ON COLUMN stores.address_detail IS '상세주소';
COMMENT ON COLUMN stores.phone IS '연락처';
COMMENT ON COLUMN stores.location IS '판매점 위치 좌표 (PostGIS POINT, EPSG:4326)';
COMMENT ON COLUMN stores.sido IS '시/도';
COMMENT ON COLUMN stores.sigungu IS '시/군/구';
COMMENT ON COLUMN stores.dong IS '읍/면/동';
COMMENT ON COLUMN stores.sells_lotto IS '로또 6/45 판매 여부';
COMMENT ON COLUMN stores.sells_pension IS '연금복권 720+ 판매 여부';
COMMENT ON COLUMN stores.sells_speetto_2000 IS '스피또2000 판매 여부';
COMMENT ON COLUMN stores.sells_speetto_1000 IS '스피또1000 판매 여부';
COMMENT ON COLUMN stores.sells_speetto_500 IS '스피또500 판매 여부';
COMMENT ON COLUMN stores.is_active IS '영업 중 여부';
COMMENT ON COLUMN stores.created_at IS '최초 등록 일시';
COMMENT ON COLUMN stores.updated_at IS '최근 수정 일시';

CREATE TABLE lotto_results (
    id                  SERIAL PRIMARY KEY,
    round_no            INTEGER UNIQUE NOT NULL,
    draw_date           DATE NOT NULL,
    num1                SMALLINT NOT NULL,
    num2                SMALLINT NOT NULL,
    num3                SMALLINT NOT NULL,
    num4                SMALLINT NOT NULL,
    num5                SMALLINT NOT NULL,
    num6                SMALLINT NOT NULL,
    bonus               SMALLINT NOT NULL,
    first_prize_amount  BIGINT DEFAULT 0,
    first_prize_winners INTEGER DEFAULT 0,
    total_sales         BIGINT DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE lotto_results IS '로또 6/45 회차별 추첨 결과';
COMMENT ON COLUMN lotto_results.id IS 'PK';
COMMENT ON COLUMN lotto_results.round_no IS '추첨 회차';
COMMENT ON COLUMN lotto_results.draw_date IS '추첨일 (매주 토요일)';
COMMENT ON COLUMN lotto_results.num1 IS '당첨번호 1 (오름차순)';
COMMENT ON COLUMN lotto_results.num2 IS '당첨번호 2';
COMMENT ON COLUMN lotto_results.num3 IS '당첨번호 3';
COMMENT ON COLUMN lotto_results.num4 IS '당첨번호 4';
COMMENT ON COLUMN lotto_results.num5 IS '당첨번호 5';
COMMENT ON COLUMN lotto_results.num6 IS '당첨번호 6';
COMMENT ON COLUMN lotto_results.bonus IS '보너스 번호 (2등 판별용)';
COMMENT ON COLUMN lotto_results.first_prize_amount IS '1등 1인당 당첨금 (원)';
COMMENT ON COLUMN lotto_results.first_prize_winners IS '1등 당첨자 수 (0이면 이월)';
COMMENT ON COLUMN lotto_results.total_sales IS '해당 회차 총 판매액 (원)';
COMMENT ON COLUMN lotto_results.created_at IS '데이터 수집 일시';


CREATE TABLE pension_results (
    id                  SERIAL PRIMARY KEY,
    round_no            INTEGER UNIQUE NOT NULL,
    draw_date           DATE NOT NULL,
    first_prize_group   SMALLINT NOT NULL,
    first_prize_number  VARCHAR(6) NOT NULL,
    bonus_number        VARCHAR(6) NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE pension_results IS '연금복권 720+ 회차별 추첨 결과';
COMMENT ON COLUMN pension_results.id IS 'PK';
COMMENT ON COLUMN pension_results.round_no IS '추첨 회차';
COMMENT ON COLUMN pension_results.draw_date IS '추첨일 (매주 목요일)';
COMMENT ON COLUMN pension_results.first_prize_group IS '1등 당첨번호의 조 (1~5)';
COMMENT ON COLUMN pension_results.first_prize_number IS '1등 당첨번호 6자리 (2~6등은 끝자리 일치로 판별)';
COMMENT ON COLUMN pension_results.bonus_number IS '보너스 당첨번호 6자리 (1등과 별도 추첨, 각 조 공통)';
COMMENT ON COLUMN pension_results.created_at IS '데이터 수집 일시';


CREATE TABLE speetto_games (
    id                      SERIAL PRIMARY KEY,
    game_id                 VARCHAR(30) UNIQUE NOT NULL,
    name                    VARCHAR(50) NOT NULL,
    game_type               VARCHAR(10) NOT NULL,
    round_no                INTEGER NOT NULL,
    price                   INTEGER NOT NULL,
    is_on_sale              BOOLEAN DEFAULT TRUE,
    total_first_prizes      INTEGER DEFAULT 0,
    remaining_first_prizes  INTEGER DEFAULT 0,
    total_second_prizes     INTEGER DEFAULT 0,
    remaining_second_prizes INTEGER DEFAULT 0,
    intake_rate             SMALLINT DEFAULT 0,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX ix_speetto_type ON speetto_games (game_type);

CREATE TRIGGER trg_speetto_games_updated_at
    BEFORE UPDATE ON speetto_games
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE speetto_games IS '스피또(500/1000/2000) 회차별 발행·잔여 현황';
COMMENT ON COLUMN speetto_games.id IS 'PK';
COMMENT ON COLUMN speetto_games.game_id IS '종류+회차 조합 키 (예: st2000_67)';
COMMENT ON COLUMN speetto_games.name IS '게임명 (예: 스피또2000 67회)';
COMMENT ON COLUMN speetto_games.game_type IS '스피또 종류 (st2000, st1000, st500)';
COMMENT ON COLUMN speetto_games.round_no IS '회차 번호 (종류별 독립 채번)';
COMMENT ON COLUMN speetto_games.price IS '1장 가격 (원)';
COMMENT ON COLUMN speetto_games.is_on_sale IS '현재 판매 중 여부 (1등 소진 시 FALSE)';
COMMENT ON COLUMN speetto_games.total_first_prizes IS '해당 회차 1등 총 발행 매수';
COMMENT ON COLUMN speetto_games.remaining_first_prizes IS '1등 잔여 매수';
COMMENT ON COLUMN speetto_games.total_second_prizes IS '2등 총 발행 매수';
COMMENT ON COLUMN speetto_games.remaining_second_prizes IS '2등 잔여 매수';
COMMENT ON COLUMN speetto_games.intake_rate IS '전국 판매점 입고율 (%)';
COMMENT ON COLUMN speetto_games.updated_at IS '최근 크롤링 갱신 일시';


CREATE TABLE winning_stores (
    id              SERIAL PRIMARY KEY,
    lottery_type    VARCHAR(20) NOT NULL,
    round_no        INTEGER NOT NULL,
    prize_rank      SMALLINT NOT NULL,
    store_id        INTEGER REFERENCES stores(id) ON DELETE SET NULL,
    store_name      VARCHAR(100) NOT NULL,
    store_address   VARCHAR(255) NOT NULL,
    prize_amount    BIGINT DEFAULT 0,
    purchase_method VARCHAR(20) DEFAULT 'unknown',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (lottery_type, round_no, prize_rank, store_name)
);

CREATE INDEX ix_winning_type_round ON winning_stores (lottery_type, round_no);
CREATE INDEX ix_winning_store ON winning_stores (store_id, lottery_type);

COMMENT ON TABLE winning_stores IS '복권 종류 통합 당첨 판매점 이력';
COMMENT ON COLUMN winning_stores.id IS 'PK';
COMMENT ON COLUMN winning_stores.lottery_type IS '복권 종류 (lotto, pension, speetto)';
COMMENT ON COLUMN winning_stores.round_no IS '당첨 회차';
COMMENT ON COLUMN winning_stores.prize_rank IS '당첨 등수 (1, 2, 3)';
COMMENT ON COLUMN winning_stores.store_id IS '판매점 FK (매칭 실패 시 NULL)';
COMMENT ON COLUMN winning_stores.store_name IS '크롤링 원본 판매점명';
COMMENT ON COLUMN winning_stores.store_address IS '크롤링 원본 판매점 주소';
COMMENT ON COLUMN winning_stores.prize_amount IS '당첨금 (원)';
COMMENT ON COLUMN winning_stores.purchase_method IS '구매 방식 (auto, manual, semi_auto, unknown)';
COMMENT ON COLUMN winning_stores.created_at IS '데이터 수집 일시';


CREATE TABLE crawl_logs (
    id          SERIAL PRIMARY KEY,
    task_name   VARCHAR(100) NOT NULL,
    status      VARCHAR(20) DEFAULT 'running',
    message     TEXT DEFAULT '',
    started_at  TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX ix_crawl_logs_task ON crawl_logs (task_name, started_at DESC);

COMMENT ON TABLE crawl_logs IS '크롤링 실행 이력 (모니터링/디버깅용)';
COMMENT ON COLUMN crawl_logs.id IS 'PK';
COMMENT ON COLUMN crawl_logs.task_name IS '태스크명 (crawl_lotto_results 등)';
COMMENT ON COLUMN crawl_logs.status IS '실행 상태 (running, success, failed)';
COMMENT ON COLUMN crawl_logs.message IS '결과 메시지 또는 에러 내용';
COMMENT ON COLUMN crawl_logs.started_at IS '실행 시작 시각';
COMMENT ON COLUMN crawl_logs.finished_at IS '실행 종료 시각 (실행 중이면 NULL)';