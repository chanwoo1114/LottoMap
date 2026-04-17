import argparse
import asyncio
import glob
from pathlib import Path

import asyncpg

BASE_DIR = Path(__file__).resolve().parent.parent


async def main(args):
    from app.core.config import settings

    conn = await asyncpg.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
    )

    if args.reset:
        await conn.execute("""
            DROP TABLE IF EXISTS crawl_logs CASCADE;
            DROP TABLE IF EXISTS winning_stores CASCADE;
            DROP TABLE IF EXISTS speetto_games CASCADE;
            DROP TABLE IF EXISTS pension_results CASCADE;
            DROP TABLE IF EXISTS lotto_results CASCADE;
            DROP TABLE IF EXISTS stores CASCADE;
        """)

    sql_files = sorted(glob.glob(str(BASE_DIR / "sql/*.sql")))
    for f in sql_files:
        with open(f) as fp:
            await conn.execute(fp.read())

    await conn.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--reset", action="store_true", help="기존 테이블 DROP 후 재생성")
    asyncio.run(main(p.parse_args()))