"""로또 생성기 공통 상수·헬퍼 (statistical/ai 공유)"""
from collections import Counter
from itertools import combinations

TOTAL = 45
PICK = 6
SECTIONS = [(1, 10), (11, 20), (21, 30), (31, 40), (41, 45)]


def calc_ac(nums) -> int:
    """조합의 AC값(서로 다른 차이 개수) 계산"""
    return len(set(abs(a - b) for a, b in combinations(nums, 2))) - (PICK - 1)


def count_consecutive_pairs(sorted_nums) -> int:
    """정렬된 번호에서 연속(n, n+1) 쌍의 개수"""
    return sum(
        1 for i in range(len(sorted_nums) - 1)
        if sorted_nums[i + 1] == sorted_nums[i] + 1
    )


def base_validate(nums) -> bool:
    """두 생성기 공통 구조 검증(홀짝·23분할·연번4연속·구간4·끝수3중복)"""
    odds = sum(1 for n in nums if n % 2 == 1)
    if odds <= 1 or odds >= 5:
        return False
    if sum(1 for n in nums if n >= 23) in (0, 6):
        return False
    sn = sorted(nums)
    c = 1
    for i in range(1, len(sn)):
        c = c + 1 if sn[i] == sn[i - 1] + 1 else 1
        if c >= 4:
            return False
    for s, e in SECTIONS:
        if sum(1 for n in nums if s <= n <= e) >= 4:
            return False
    if max(Counter(n % 10 for n in nums).values()) >= 3:
        return False
    return True
