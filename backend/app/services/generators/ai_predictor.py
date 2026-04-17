"""고도화 AI 로또 번호 생성기 v3 (asyncpg) - 7모델 앙상블 + 몬테카를로"""
import math
import random
from collections import Counter, defaultdict
from itertools import combinations

import asyncpg

TOTAL = 45; PICK = 6
PRIMES = {2,3,5,7,11,13,17,19,23,29,31,37,41,43}
SECTIONS = [(1,10),(11,20),(21,30),(31,40),(41,45)]


class AIGeneratorV3:
    MONTE_CARLO_N = 5000

    def __init__(self):
        self._rounds = []; self._trained = False
        self._trans = {}; self._cycle = {}; self._affinity = {}
        self._trend = {}; self._pos = {}
        self._gap_accel = {}
        self._consec_dist = {}
        self._weights = {
            "markov": 0.18, "cycle": 0.22, "cluster": 0.12,
            "trend": 0.15, "position": 0.10, "gap_accel": 0.13, "consecutive": 0.10,
        }
        self._backtest = {}

    async def train(self, pool: asyncpg.Pool):
        rows = await pool.fetch("SELECT round_no, num1, num2, num3, num4, num5, num6 FROM lotto_results ORDER BY round_no ASC")
        if len(rows) < 30: return
        self._rounds = [(r["round_no"], sorted([r["num1"],r["num2"],r["num3"],r["num4"],r["num5"],r["num6"]])) for r in rows]
        self._train_markov()
        self._train_cycle()
        self._train_cluster()
        self._train_trend()
        self._train_position()
        self._train_gap_accel()
        self._train_consecutive()
        self._run_backtest()
        self._trained = True

    def _train_markov(self):
        """시간 감쇠 적용 2차 마르코프 체인"""
        tc = defaultdict(Counter)
        n_rounds = len(self._rounds)
        for i in range(2, n_rounds):
            decay = 0.97 ** (n_rounds - 1 - i)
            p2 = set(self._rounds[i-2][1]); p1 = self._rounds[i-1][1]; cur = self._rounds[i][1]
            for a in p1:
                w = 1.3 if a in p2 else 1.0
                for b in cur: tc[a][b] += w * decay
        self._trans = {a: {b: c/sum(ct.values()) for b, c in ct.items()} for a, ct in tc.items()}

    def _train_cycle(self):
        apps = defaultdict(list)
        for i, (_, nums) in enumerate(self._rounds):
            for n in nums: apps[n].append(i)
        t = len(self._rounds)
        for n in range(1, TOTAL+1):
            idxs = apps.get(n, [])
            if len(idxs) >= 3:
                gaps = [idxs[j]-idxs[j-1] for j in range(1, len(idxs))]
                avg = sum(gaps)/len(gaps); std = math.sqrt(sum((g-avg)**2 for g in gaps)/len(gaps)) or 1.0
            else: avg = TOTAL/PICK; std = 3.0
            self._cycle[n] = {"avg": avg, "std": std, "since": t-1-(idxs[-1] if idxs else 0)}

    def _train_cluster(self):
        """시간 감쇠 적용 PMI + Jaccard 친화도"""
        freq = Counter(); co = defaultdict(Counter)
        n_rounds = len(self._rounds)
        for i, (_, nums) in enumerate(self._rounds):
            decay = 0.98 ** (n_rounds - 1 - i)
            for n in nums: freq[n] += decay
            for a in nums:
                for b in nums:
                    if a != b: co[a][b] += decay
        t = sum(freq.values()) / TOTAL
        self._affinity = {}
        for a in range(1, TOTAL+1):
            self._affinity[a] = {}
            for b in range(1, TOTAL+1):
                if a == b: continue
                c = co[a].get(b, 0); p_ab = c/max(t,1); p_a = freq.get(a,1)/max(t,1); p_b = freq.get(b,1)/max(t,1)
                pmi = math.log2(p_ab/(p_a*p_b)) if p_a*p_b > 0 and p_ab > 0 else -2
                jac = c/(freq.get(a,1)+freq.get(b,1)-c) if freq.get(a,0)+freq.get(b,0) > c else 0
                self._affinity[a][b] = pmi*0.6 + jac*10*0.4

    def _train_trend(self):
        """3구간 트렌드 + 모멘텀 + 가속도"""
        win = 30; recent = self._rounds[-win*3:] if len(self._rounds) > win*3 else self._rounds
        th = max(len(recent)//3, 1)
        for n in range(1, TOTAL+1):
            p1 = sum(1 for _, nums in recent[:th] if n in nums)
            p2 = sum(1 for _, nums in recent[th:th*2] if n in nums)
            p3 = sum(1 for _, nums in recent[th*2:] if n in nums)
            slope = (p3-p1)/th
            mom = p3 - p2
            accel = (p3 - p2) - (p2 - p1)
            if slope > 0.05:
                direction = "up"
            elif slope < -0.05:
                direction = "down"
            else:
                direction = "flat"
            self._trend[n] = {"slope": slope, "momentum": mom, "accel": accel, "dir": direction}

    def _train_position(self):
        """시간 감쇠 적용 포지션별 출현 확률"""
        pf = defaultdict(Counter)
        n_rounds = len(self._rounds)
        for i, (_, nums) in enumerate(self._rounds):
            decay = 0.98 ** (n_rounds - 1 - i)
            for pos, n in enumerate(sorted(nums)): pf[pos][n] += decay
        for pos in range(PICK):
            t = sum(pf[pos].values()) or 1
            for n in range(1, TOTAL+1): pf[pos][n] = pf[pos].get(n, 0) / t
        self._pos = dict(pf)

    def _train_gap_accel(self):
        """번호별 갭 가속도: 출현 간격이 줄어드는지 늘어나는지"""
        apps = defaultdict(list)
        for i, (_, nums) in enumerate(self._rounds):
            for n in nums: apps[n].append(i)

        for n in range(1, TOTAL+1):
            idxs = apps.get(n, [])
            if len(idxs) < 4:
                self._gap_accel[n] = {"accel": 0.0, "recent_gap_trend": "stable"}
                continue
            gaps = [idxs[j]-idxs[j-1] for j in range(1, len(idxs))]
            recent_gaps = gaps[-min(5, len(gaps)):]
            if len(recent_gaps) >= 2:
                diffs = [recent_gaps[j]-recent_gaps[j-1] for j in range(1, len(recent_gaps))]
                accel = sum(diffs) / len(diffs)
            else:
                accel = 0.0
            if accel < -0.5:
                trend = "accelerating"
            elif accel > 0.5:
                trend = "decelerating"
            else:
                trend = "stable"
            self._gap_accel[n] = {"accel": accel, "recent_gap_trend": trend}

    def _train_consecutive(self):
        """실제 당첨번호의 연번 쌍 수 분포 학습"""
        consec_counts = Counter()
        for _, nums in self._rounds:
            sn = sorted(nums)
            pairs = sum(1 for i in range(len(sn)-1) if sn[i+1] == sn[i]+1)
            consec_counts[pairs] += 1
        t = len(self._rounds) or 1
        self._consec_dist = {k: v/t for k, v in consec_counts.items()}

    def _run_backtest(self):
        """슬라이딩 윈도우 백테스트로 모델별 가중치 동적 조정"""
        if len(self._rounds) < 100: return
        window = min(80, len(self._rounds) // 2)
        hits = {m: 0.0 for m in self._weights}

        for i in range(len(self._rounds)-window, len(self._rounds)):
            if i < 2: continue
            recency_w = 0.98 ** (len(self._rounds) - 1 - i)
            p1, p2 = self._rounds[i-1][1], self._rounds[i-2][1]
            actual = set(self._rounds[i][1])

            scorers = {
                "markov": self._markov(p1, p2),
                "cycle": self._cyc_scores(),
                "trend": self._trend_scores(),
                "gap_accel": self._gap_accel_scores(),
            }
            for name, scores in scorers.items():
                top6 = sorted(scores, key=scores.get, reverse=True)[:6]
                hits[name] += len(set(top6) & actual) * recency_w

        avg = sum(hits.values()) / max(len(hits)-3, 1)
        for m in self._weights:
            if m not in hits: hits[m] = avg

        th = sum(hits.values()) or 1
        for m in self._weights:
            self._weights[m] = self._weights[m]*0.3 + (hits.get(m, avg)/th)*0.7
        wt = sum(self._weights.values())
        self._weights = {m: w/wt for m, w in self._weights.items()}
        self._backtest = {"hits": {m: round(v, 2) for m, v in hits.items()}, "weights": {m: round(w, 4) for m, w in self._weights.items()}}

    def _markov(self, p1, p2):
        sc = defaultdict(float)
        for a in p1:
            w = 1.3 if a in p2 else 1.0
            for b, prob in self._trans.get(a, {}).items(): sc[b] += prob * w
        t = sum(sc.values()) or 1
        return {n: sc.get(n, 0.001)/t for n in range(1, TOTAL+1)}

    def _cyc_scores(self):
        sc = {}
        for n in range(1, TOTAL+1):
            d = self._cycle.get(n, {"avg": 7.5, "std": 3.0, "since": 0})
            r = d["since"]/d["avg"] if d["avg"] > 0 else 0
            if r < 0.4: sc[n] = 0.15
            elif r <= 1.3: sc[n] = math.exp(-0.5*((r-1.0)/0.35)**2)
            else: sc[n] = 0.65 + min((d["since"]-d["avg"])/max(d["std"],1)*0.15, 0.35)
        t = sum(sc.values()) or 1
        return {n: s/t for n, s in sc.items()}

    def _cluster_scores(self, picked):
        sc = {n: 1.0 for n in range(1, TOTAL+1)}
        for ex in picked:
            for pt, s in self._affinity.get(ex, {}).items():
                if pt not in picked: sc[pt] += max(s, 0)*0.5
        t = sum(sc.values()) or 1
        return {n: s/t for n, s in sc.items()}

    def _trend_scores(self):
        sc = {}
        for n in range(1, TOTAL+1):
            td = self._trend.get(n, {})
            slope = td.get("slope", 0)
            accel = td.get("accel", 0)
            base = 1.0 + slope * 3
            if accel > 0 and slope > 0:
                base *= 1.2
            elif accel < 0 and slope < 0:
                base *= 0.8
            sc[n] = max(base, 0.1)
        t = sum(sc.values()) or 1
        return {n: s/t for n, s in sc.items()}

    def _gap_accel_scores(self):
        """갭 가속도 기반 점수: 간격이 줄어드는 번호에 가중치"""
        sc = {}
        for n in range(1, TOTAL+1):
            ga = self._gap_accel.get(n, {"accel": 0.0})
            accel = ga["accel"]
            cycle = self._cycle.get(n, {"since": 0, "avg": 7.5})
            ratio = cycle["since"] / cycle["avg"] if cycle["avg"] > 0 else 0
            if accel < -0.5 and ratio > 0.7:
                sc[n] = 1.5 + abs(accel) * 0.3
            elif accel < 0:
                sc[n] = 1.2
            elif accel > 0.5:
                sc[n] = 0.6
            else:
                sc[n] = 1.0
        t = sum(sc.values()) or 1
        return {n: s/t for n, s in sc.items()}

    def _consecutive_score(self, nums):
        """연번 쌍 수가 실제 분포에 부합하는 정도"""
        sn = sorted(nums)
        pairs = sum(1 for i in range(len(sn)-1) if sn[i+1] == sn[i]+1)
        return self._consec_dist.get(pairs, 0.01)

    def _ensemble(self, p1, p2, picked, pos):
        mk = self._markov(p1, p2); cy = self._cyc_scores(); cl = self._cluster_scores(picked)
        tr = self._trend_scores(); ps = self._pos.get(pos, {n: 1/TOTAL for n in range(1, TOTAL+1)})
        ga = self._gap_accel_scores()
        w = self._weights
        comb = {}
        for n in range(1, TOTAL+1):
            comb[n] = (
                mk.get(n,0)*w["markov"] + cy.get(n,0)*w["cycle"] +
                cl.get(n,0)*w["cluster"] + tr.get(n,0)*w["trend"] +
                ps.get(n,0)*w["position"] + ga.get(n,0)*w["gap_accel"]
            )
        t = sum(comb.values()) or 1
        return {n: s/t for n, s in comb.items()}

    def _pick(self, scores, exclude, temp):
        cands = {n: s for n, s in scores.items() if n not in exclude and s > 0}
        if not cands: return random.choice([n for n in range(1, TOTAL+1) if n not in exclude])
        ns, raw = list(cands.keys()), [cands[n] for n in cands]
        mx = max(raw); exp = [math.exp((s-mx)/temp) for s in raw]; t = sum(exp)
        return random.choices(ns, weights=[e/t for e in exp], k=1)[0]

    @staticmethod
    def _calc_ac(nums): return len(set(abs(a-b) for a, b in combinations(nums, 2))) - (PICK-1)

    def _validate(self, nums):
        s = sum(nums)
        if not (95 <= s <= 180): return False
        odds = sum(1 for n in nums if n % 2 == 1)
        if odds <= 1 or odds >= 5: return False
        if sum(1 for n in nums if n >= 23) in (0, 6): return False
        if self._calc_ac(nums) < 6: return False
        sn = sorted(nums); c = 1
        for i in range(1, len(sn)):
            c = c+1 if sn[i] == sn[i-1]+1 else 1
            if c >= 4: return False
        for s2, e in SECTIONS:
            if sum(1 for n in nums if s2 <= n <= e) >= 4: return False
        if max(Counter(n % 10 for n in nums).values()) >= 3: return False
        return True

    async def generate(self, pool, count=5, temperature=1.5, exclude_numbers=None, include_numbers=None):
        if not self._trained: await self.train(pool)
        if not self._rounds: return [{"error": "데이터 없음"}]

        p1, p2 = self._rounds[-1][1], self._rounds[-2][1] if len(self._rounds) >= 2 else self._rounds[-1][1]
        ex, inc = set(exclude_numbers or []), set(include_numbers or [])

        candidates = []
        for step in range(self.MONTE_CARLO_N):
            temp = temperature * (1.0 - 0.3 * step / self.MONTE_CARLO_N)
            picked = list(inc)
            for pos in range(len(picked), PICK):
                sc = self._ensemble(p1, p2, picked, pos)
                for n in ex: sc[n] = 0.0
                picked.append(self._pick(sc, set(picked)|ex, temp))
            picked = sorted(picked)
            if not self._validate(picked): continue
            fs = self._ensemble(p1, p2, picked, 0)
            base_score = sum(fs.get(n, 0) for n in picked)
            consec_bonus = self._consecutive_score(picked)
            final_score = base_score * (1.0 + consec_bonus * 0.5)
            candidates.append((picked, final_score))

        seen = set(); results = []
        for nums, score in sorted(candidates, key=lambda x: x[1], reverse=True):
            if tuple(nums) in seen: continue
            seen.add(tuple(nums))
            odds = sum(1 for n in nums if n % 2 == 1)
            sn = sorted(nums)
            consec_pairs = sum(1 for i in range(len(sn)-1) if sn[i+1] == sn[i]+1)
            results.append({
                "numbers": nums,
                "confidence": min(99, max(1, int(score*TOTAL*80))),
                "sum": sum(nums),
                "ac_value": self._calc_ac(nums),
                "odd_even": f"{odds}:{6-odds}",
                "consecutive_pairs": consec_pairs,
                "number_details": {
                    str(n): {
                        "trend": self._trend.get(n, {}).get("dir", "flat"),
                        "gap_trend": self._gap_accel.get(n, {}).get("recent_gap_trend", "stable"),
                        "cycle_since": self._cycle.get(n, {}).get("since", 0),
                    } for n in nums
                },
                "model_weights": {m: round(w, 4) for m, w in self._weights.items()},
            })
            if len(results) >= count: break
        return results

    async def get_full_insight(self, pool):
        if not self._trained: await self.train(pool)
        if not self._rounds: return {"error": "데이터 없음"}
        p1, p2 = self._rounds[-1][1], (self._rounds[-2][1] if len(self._rounds) >= 2 else self._rounds[-1][1])

        ens = self._ensemble(p1, p2, [], 0)
        ens_top = sorted(ens.items(), key=lambda x: x[1], reverse=True)[:15]

        up = sorted([{"number": n, "slope": round(d["slope"], 4), "accel": round(d["accel"], 4)}
                     for n, d in self._trend.items() if d["dir"] == "up"],
                    key=lambda x: x["slope"], reverse=True)[:10]
        down = sorted([{"number": n, "slope": round(d["slope"], 4), "accel": round(d["accel"], 4)}
                       for n, d in self._trend.items() if d["dir"] == "down"],
                      key=lambda x: x["slope"])[:10]
        od = sorted([{"number": n, "avg_cycle": round(d["avg"], 1), "since": d["since"],
                      "overdue": round(d["since"]/d["avg"], 2) if d["avg"] > 0 else 0}
                     for n, d in self._cycle.items() if d["avg"] > 0 and d["since"]/d["avg"] > 1.3],
                    key=lambda x: x["overdue"], reverse=True)[:10]
        accel = sorted([{"number": n, "gap_accel": round(d["accel"], 2), "trend": d["recent_gap_trend"]}
                        for n, d in self._gap_accel.items() if d["recent_gap_trend"] == "accelerating"],
                       key=lambda x: x["gap_accel"])[:10]

        return {
            "based_on_round": self._rounds[-1][0], "total_rounds": len(self._rounds),
            "previous_numbers": p1, "model_weights": {m: round(w, 4) for m, w in self._weights.items()},
            "backtest": self._backtest,
            "ensemble_top15": [{"number": n, "score": round(s*100, 2)} for n, s in ens_top],
            "trending_up": up, "trending_down": down, "overdue_numbers": od,
            "accelerating_numbers": accel,
            "consecutive_distribution": {str(k): round(v, 3) for k, v in sorted(self._consec_dist.items())},
        }
