"""고도화 통계 기반 로또 번호 생성기 v3 (asyncpg)"""
import math
import random
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import combinations

import asyncpg

TOTAL = 45
PICK = 6
PRIMES = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43}
SECTIONS = [(1, 10), (11, 20), (21, 30), (31, 40), (41, 45)]


@dataclass
class NumberProfile:
    number: int; total_freq: int = 0; recent_freq: int = 0
    weighted_freq: float = 0.0; gap_since_last: int = 0
    avg_cycle: float = 0.0; cycle_std: float = 0.0
    overdue_ratio: float = 0.0; temperature: str = "warm"
    streak: int = 0  # 양수=연속출현, 음수=연속미출현
    last_digit_freq: float = 0.0


class StatisticalGeneratorV3:
    RECENT_WINDOW = 50
    STRATEGIES = ["hot", "cold", "balanced", "overdue", "pattern_match", "contrarian", "streak_based"]

    def __init__(self):
        self.profiles: dict[int, NumberProfile] = {}
        self.pattern_sum_range = (100, 175)
        self.pattern_avg_sum = 0.0
        self.pattern_ac_range = (7, 10)
        self.pattern_avg_ac = 0.0
        self.pattern_odd_dist = {}
        self.pattern_section_patterns = []
        self.pattern_digit_dist = {}
        self.pmi: dict[tuple, float] = {}
        self.top_partners: dict[int, list] = {}
        self._rounds = []
        self._loaded = False

    async def load_data(self, pool: asyncpg.Pool):
        rows = await pool.fetch("SELECT round_no, num1, num2, num3, num4, num5, num6 FROM lotto_results ORDER BY round_no ASC")
        if len(rows) < 20:
            return
        self._rounds = [(r["round_no"], sorted([r["num1"], r["num2"], r["num3"], r["num4"], r["num5"], r["num6"]])) for r in rows]
        self._build_profiles()
        self._build_pattern()
        self._build_pmi()
        self._loaded = True

    def _build_profiles(self):
        total_rounds = len(self._rounds)
        appearances = defaultdict(list)
        for i, (_, nums) in enumerate(self._rounds):
            for n in nums: appearances[n].append(i)

        recent_c = Counter()
        for _, nums in self._rounds[-self.RECENT_WINDOW:]: recent_c.update(nums)

        last_round_nums = set(self._rounds[-1][1]) if self._rounds else set()

        for n in range(1, TOTAL + 1):
            p = NumberProfile(number=n)
            idxs = appearances.get(n, [])
            p.total_freq = len(idxs)
            p.recent_freq = recent_c.get(n, 0)
            p.weighted_freq = sum(0.95 ** (total_rounds - 1 - i) for i in idxs)
            p.gap_since_last = (total_rounds - 1 - idxs[-1]) if idxs else total_rounds

            if len(idxs) >= 3:
                gaps = [idxs[j] - idxs[j-1] for j in range(1, len(idxs))]
                p.avg_cycle = sum(gaps) / len(gaps)
                p.cycle_std = math.sqrt(sum((g - p.avg_cycle)**2 for g in gaps) / len(gaps)) or 1.0
            else:
                p.avg_cycle = TOTAL / PICK; p.cycle_std = 3.0

            p.overdue_ratio = p.gap_since_last / p.avg_cycle if p.avg_cycle > 0 else 0
            p.temperature = "hot" if p.recent_freq >= 8 else "warm" if p.recent_freq >= 5 else "cold" if p.recent_freq >= 2 else "ice"

            # 스트릭 계산: 최근 연속 출현/미출현 횟수
            streak = 0
            for _, nums in reversed(self._rounds):
                if n in nums:
                    if streak <= 0 and streak != 0: break
                    streak += 1
                else:
                    if streak > 0: break
                    streak -= 1
            p.streak = streak

            # 끝수 빈도
            digit = n % 10
            same_digit = [m for m in range(1, TOTAL+1) if m % 10 == digit and m != n]
            p.last_digit_freq = recent_c.get(n, 0) / max(sum(recent_c.get(m, 0) for m in same_digit), 1)

            self.profiles[n] = p

    def _build_pattern(self):
        sums, acs = [], []
        odd_c, sec_c, digit_c = Counter(), Counter(), Counter()
        for _, nums in self._rounds:
            sums.append(sum(nums))
            diffs = set()
            for a, b in combinations(nums, 2): diffs.add(abs(a - b))
            acs.append(len(diffs) - (PICK - 1))
            odd_c[sum(1 for n in nums if n % 2 == 1)] += 1
            sec_c[tuple(sum(1 for n in nums if s <= n <= e) for s, e in SECTIONS)] += 1
            digits = Counter(n % 10 for n in nums)
            digit_c[max(digits.values())] += 1

        t = len(self._rounds); ss = sorted(sums); sa = sorted(acs)
        self.pattern_sum_range = (ss[int(t * 0.1)], ss[int(t * 0.9)])
        self.pattern_avg_sum = sum(sums) / t
        self.pattern_ac_range = (sa[int(t * 0.1)], sa[int(t * 0.9)])
        self.pattern_avg_ac = sum(acs) / t
        self.pattern_odd_dist = {k: v / t for k, v in odd_c.items()}
        self.pattern_section_patterns = sec_c.most_common(30)
        self.pattern_digit_dist = {k: v / t for k, v in digit_c.items()}

    def _build_pmi(self):
        """시간 감쇠 적용 PMI"""
        t = len(self._rounds)
        freq = Counter()
        pair_c = Counter()
        for i, (_, nums) in enumerate(self._rounds):
            decay = 0.98 ** (t - 1 - i)
            for n in nums: freq[n] += decay
            for pair in combinations(sorted(nums), 2): pair_c[pair] += decay

        total_w = sum(freq.values()) / TOTAL
        for (a, b), c in pair_c.items():
            p_ab = c / max(total_w, 1); p_a = freq.get(a, 1) / max(total_w, 1); p_b = freq.get(b, 1) / max(total_w, 1)
            pmi = math.log2(p_ab / (p_a * p_b)) if p_a * p_b > 0 and p_ab > 0 else 0
            self.pmi[(a, b)] = pmi; self.pmi[(b, a)] = pmi

        for n in range(1, TOTAL + 1):
            partners = sorted([(m, self.pmi.get((min(n,m), max(n,m)), 0)) for m in range(1, TOTAL+1) if m != n], key=lambda x: x[1], reverse=True)
            self.top_partners[n] = partners[:5]

    @staticmethod
    def _calc_ac(nums): return len(set(abs(a-b) for a, b in combinations(nums, 2))) - (PICK - 1)

    def _build_weights(self, strategy):
        w = {n: 1.0 for n in range(1, TOTAL + 1)}
        if strategy == "hot":
            mx = max(p.weighted_freq for p in self.profiles.values()) or 1
            for n, p in self.profiles.items(): w[n] = 0.5 + (p.weighted_freq / mx) * 2.5
        elif strategy == "cold":
            for n, p in self.profiles.items(): w[n] = 1.0 + min(p.overdue_ratio, 3.0) * 1.5 if p.overdue_ratio > 1.0 else 0.3
        elif strategy == "overdue":
            for n, p in self.profiles.items():
                w[n] = 1.0 + max(0, (p.gap_since_last - p.avg_cycle) / max(p.cycle_std, 1)) * 1.2 if p.overdue_ratio > 1.2 else 0.2
        elif strategy == "contrarian":
            for n, p in self.profiles.items(): w[n] = {"hot": 0.3, "warm": 1.0, "cold": 2.0, "ice": 2.5}[p.temperature]
        elif strategy == "streak_based":
            for n, p in self.profiles.items():
                if p.streak >= 2:
                    w[n] = 1.5 + p.streak * 0.3
                elif p.streak <= -5:
                    w[n] = 1.0 + min(abs(p.streak) * 0.15, 1.5)
                elif p.streak < 0:
                    w[n] = 0.5
                else:
                    w[n] = 1.0
        elif strategy in ("balanced", "pattern_match"):
            hw = self._build_weights("hot")
            cw = self._build_weights("cold")
            ow = self._build_weights("overdue")
            sw = self._build_weights("streak_based")
            for n in range(1, TOTAL + 1):
                w[n] = hw[n] * 0.30 + cw[n] * 0.20 + ow[n] * 0.25 + sw[n] * 0.15 + 0.10
        return w

    def _validate(self, nums, strict=True):
        s = sum(nums); lo, hi = self.pattern_sum_range
        if not (lo <= s <= hi): return False
        ac = self._calc_ac(nums); al, ah = self.pattern_ac_range
        if strict and not (al <= ac <= ah): return False
        odds = sum(1 for n in nums if n % 2 == 1)
        if odds <= 1 or odds >= 5: return False
        if sum(1 for n in nums if n >= 23) in (0, 6): return False
        sn = sorted(nums); c = 1
        for i in range(1, len(sn)):
            c = c + 1 if sn[i] == sn[i-1] + 1 else 1
            if c >= 4: return False
        for s2, e in SECTIONS:
            if sum(1 for n in nums if s2 <= n <= e) >= 4: return False
        if max(Counter(n % 10 for n in nums).values()) >= 3: return False
        return True

    def _pattern_score(self, nums):
        sc, ck = 0.0, 0
        sc += max(0, 1.0 - abs(sum(nums) - self.pattern_avg_sum) / 50); ck += 1
        sc += max(0, 1.0 - abs(self._calc_ac(nums) - self.pattern_avg_ac) / 5); ck += 1
        sc += self.pattern_odd_dist.get(sum(1 for n in nums if n % 2 == 1), 0) * 3; ck += 1
        sec = tuple(sum(1 for n in nums if s <= n <= e) for s, e in SECTIONS)
        for p, _ in self.pattern_section_patterns[:10]:
            if sec == p: sc += 1.0; break
        ck += 1
        # 끝수 중복 패턴 점수
        digit_max = max(Counter(n % 10 for n in nums).values())
        sc += self.pattern_digit_dist.get(digit_max, 0) * 2; ck += 1
        # 연번 보너스 (1~2쌍이면 가산)
        sn = sorted(nums)
        consec = sum(1 for i in range(len(sn)-1) if sn[i+1] == sn[i]+1)
        if consec in (1, 2): sc += 0.5
        ck += 1
        return sc / ck

    async def generate(self, pool, strategy="balanced", count=5, exclude_numbers=None, include_numbers=None):
        if not self._loaded: await self.load_data(pool)
        if not self._rounds: return [{"error": "데이터 없음"}]

        bw = self._build_weights(strategy)
        ex, inc = set(exclude_numbers or []), set(include_numbers or [])
        for n in ex: bw[n] = 0.0

        results, att = [], 0
        strict = strategy == "pattern_match"
        while len(results) < count and att < count * 500:
            att += 1; picked = list(inc); cw = dict(bw)
            while len(picked) < PICK:
                for ep in picked:
                    for pt, pmi in self.top_partners.get(ep, []):
                        if pt not in picked and pmi > 0: cw[pt] = cw.get(pt, 1.0) + pmi * 0.8
                tc = Counter(n % 10 for n in picked)
                for n in range(1, TOTAL+1):
                    if n not in picked and tc.get(n % 10, 0) >= 2: cw[n] = cw.get(n, 1.0) * 0.3
                cands = {n: w for n, w in cw.items() if n not in set(picked) | ex and w > 0}
                if not cands: break
                ns, ws = list(cands.keys()), list(cands.values())
                picked.append(random.choices(ns, weights=ws, k=1)[0])

            if len(picked) != PICK: continue
            picked = sorted(picked)
            if not self._validate(picked, strict): continue
            if picked in [r["numbers"] for r in results]: continue
            ps = self._pattern_score(picked)
            if strict and ps < 0.6: continue

            odds = sum(1 for n in picked if n % 2 == 1)
            sn = sorted(picked)
            consec_pairs = sum(1 for i in range(len(sn)-1) if sn[i+1] == sn[i]+1)
            results.append({
                "numbers": picked, "sum": sum(picked), "ac_value": self._calc_ac(picked),
                "odd_even": f"{odds}:{6-odds}", "pattern_score": round(ps * 100, 1),
                "consecutive_pairs": consec_pairs,
                "number_temperatures": {n: self.profiles[n].temperature for n in picked},
                "number_streaks": {n: self.profiles[n].streak for n in picked},
            })
        results.sort(key=lambda x: x["pattern_score"], reverse=True)
        return results

    async def get_full_analysis(self, pool):
        if not self._loaded: await self.load_data(pool)
        if not self.profiles: return {"error": "데이터 없음"}

        temp_groups = defaultdict(list)
        for n, p in self.profiles.items():
            temp_groups[p.temperature].append({
                "number": n, "recent_freq": p.recent_freq,
                "weighted_freq": round(p.weighted_freq, 2),
                "gap": p.gap_since_last, "avg_cycle": round(p.avg_cycle, 1),
                "overdue_ratio": round(p.overdue_ratio, 2), "streak": p.streak,
            })
        for t in temp_groups:
            temp_groups[t].sort(key=lambda x: x["weighted_freq" if t == "hot" else "overdue_ratio"], reverse=True)

        overdue = sorted([{"number": n, "avg_cycle": round(p.avg_cycle, 1), "gap": p.gap_since_last,
                          "overdue_ratio": round(p.overdue_ratio, 2)}
                         for n, p in self.profiles.items() if p.overdue_ratio > 1.3],
                        key=lambda x: x["overdue_ratio"], reverse=True)

        hot_streaks = sorted([{"number": n, "streak": p.streak}
                             for n, p in self.profiles.items() if p.streak >= 2],
                            key=lambda x: x["streak"], reverse=True)
        cold_streaks = sorted([{"number": n, "streak": p.streak}
                              for n, p in self.profiles.items() if p.streak <= -5],
                             key=lambda x: x["streak"])

        seen = set(); top_pairs = []
        for (a, b), pmi in sorted(self.pmi.items(), key=lambda x: x[1], reverse=True):
            key = (min(a, b), max(a, b))
            if key not in seen: seen.add(key); top_pairs.append({"pair": list(key), "pmi": round(pmi, 3)})
            if len(top_pairs) >= 15: break

        return {
            "based_on_round": self._rounds[-1][0], "total_rounds": len(self._rounds),
            "previous_numbers": self._rounds[-1][1], "temperature_groups": dict(temp_groups),
            "overdue_numbers": overdue[:15], "top_pairs_pmi": top_pairs,
            "hot_streaks": hot_streaks[:10], "cold_streaks": cold_streaks[:10],
            "pattern_stats": {
                "sum_range": list(self.pattern_sum_range), "avg_sum": round(self.pattern_avg_sum, 1),
                "ac_range": list(self.pattern_ac_range), "avg_ac": round(self.pattern_avg_ac, 1),
            },
        }
