"""연금복권 720+ 번호 생성기.

각 자리(1~6)가 독립적으로 0~9 중 추첨되므로 로또와 달리 조합 구조가 없다.
역대 1등 번호의 자리별 숫자 빈도와 조 빈도를 이용해 통계 기반 생성 지원.
"""
import random
from collections import Counter

import asyncpg

GROUPS = (1, 2, 3, 4, 5)
DIGITS = tuple(range(10))
NUM_LEN = 6
STRATEGIES = ("hot", "cold", "balanced", "random")


class PensionGenerator:
    RECENT_WINDOW = 30

    def __init__(self) -> None:
        self._records: list[tuple[int, int, str]] = []  # (round_no, group, number)
        self._group_freq: Counter = Counter()
        self._digit_freq: list[Counter] = [Counter() for _ in range(NUM_LEN)]
        self._recent_digit_freq: list[Counter] = [Counter() for _ in range(NUM_LEN)]
        self._loaded = False

    async def load_data(self, pool: asyncpg.Pool) -> None:
        rows = await pool.fetch(
            """
            SELECT round_no, first_prize_group, first_prize_number
            FROM pension_results
            ORDER BY round_no ASC
            """
        )
        if not rows:
            return

        self._records = [
            (r["round_no"], r["first_prize_group"], r["first_prize_number"])
            for r in rows
        ]

        for _, g, num in self._records:
            self._group_freq[g] += 1
            for pos, ch in enumerate(num):
                self._digit_freq[pos][int(ch)] += 1

        for _, _, num in self._records[-self.RECENT_WINDOW:]:
            for pos, ch in enumerate(num):
                self._recent_digit_freq[pos][int(ch)] += 1

        self._loaded = True

    def _group_weights(self, strategy: str) -> dict[int, float]:
        if strategy == "random" or not self._group_freq:
            return {g: 1.0 for g in GROUPS}
        mx = max(self._group_freq.values()) or 1
        if strategy == "hot":
            return {g: (self._group_freq.get(g, 0) / mx) ** 2 + 0.1 for g in GROUPS}
        if strategy == "cold":
            return {g: 1.0 / (self._group_freq.get(g, 0) + 1) for g in GROUPS}
        return {g: self._group_freq.get(g, 0) / mx + 0.2 for g in GROUPS}

    def _digit_weights(self, strategy: str) -> list[dict[int, float]]:
        result = []
        for pos in range(NUM_LEN):
            total_c = self._digit_freq[pos]
            recent_c = self._recent_digit_freq[pos]
            if strategy == "random" or not total_c:
                w = {d: 1.0 for d in DIGITS}
            elif strategy == "hot":
                mx = max(recent_c.values()) or 1
                w = {d: (recent_c.get(d, 0) / mx) ** 2 + 0.1 for d in DIGITS}
            elif strategy == "cold":
                w = {d: 1.0 / (recent_c.get(d, 0) + 1) for d in DIGITS}
            else:  # balanced
                mx = max(total_c.values()) or 1
                w = {d: total_c.get(d, 0) / mx + 0.2 for d in DIGITS}
            result.append(w)
        return result

    async def generate(
        self,
        pool: asyncpg.Pool,
        strategy: str = "balanced",
        count: int = 5,
        fixed_group: int | None = None,
    ) -> list[dict]:
        if not self._loaded:
            await self.load_data(pool)
        if not self._records:
            return [{"error": "데이터 없음"}]
        if strategy not in STRATEGIES:
            return [{"error": f"지원하지 않는 전략: {strategy}"}]

        gw = self._group_weights(strategy)
        dw = self._digit_weights(strategy)
        group_pool = list(gw.keys())
        group_weights = [gw[g] for g in group_pool]

        results: list[dict] = []
        seen: set[tuple[int, str]] = set()
        attempts = 0
        while len(results) < count and attempts < count * 100:
            attempts += 1
            if fixed_group is not None and fixed_group in GROUPS:
                group = fixed_group
            else:
                group = random.choices(group_pool, weights=group_weights, k=1)[0]

            digits = [
                random.choices(list(DIGITS), weights=[dw[pos][d] for d in DIGITS], k=1)[0]
                for pos in range(NUM_LEN)
            ]
            num_str = "".join(str(d) for d in digits)
            key = (group, num_str)
            if key in seen:
                continue
            seen.add(key)

            digit_counter = Counter(digits)
            results.append({
                "group": group,
                "number": num_str,
                "digits": digits,
                "sum": sum(digits),
                "unique_digits": len(digit_counter),
                "max_repeat": max(digit_counter.values()),
            })

        return results

    async def get_analysis(self, pool: asyncpg.Pool) -> dict:
        if not self._loaded:
            await self.load_data(pool)
        if not self._records:
            return {"error": "데이터 없음"}

        total = len(self._records)
        group_dist = {
            g: {
                "count": self._group_freq.get(g, 0),
                "ratio": round(self._group_freq.get(g, 0) / total, 3),
            }
            for g in GROUPS
        }

        digit_stats = []
        for pos in range(NUM_LEN):
            c = self._digit_freq[pos]
            top3 = [{"digit": d, "count": cnt} for d, cnt in c.most_common(3)]
            recent_top3 = [
                {"digit": d, "count": cnt}
                for d, cnt in self._recent_digit_freq[pos].most_common(3)
            ]
            digit_stats.append({
                "position": pos + 1,
                "top3_all_time": top3,
                "top3_recent": recent_top3,
            })

        return {
            "total_rounds": total,
            "latest_round": self._records[-1][0],
            "group_distribution": group_dist,
            "digit_stats_by_position": digit_stats,
            "recent_window": self.RECENT_WINDOW,
        }