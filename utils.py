# utils.py
import re
from typing import Optional

def parse_hhmmss_to_seconds(text: str) -> Optional[int]:
    """
    "HH:MM:SS" 또는 "MM:SS" 형태를 초(int)로 변환.
    파싱 실패 시 None
    """
    if not text:
        return None
    t = text.strip()
    # HH:MM:SS
    m = re.fullmatch(r"(?:(\d{1,2}):)?([0-5]?\d):([0-5]?\d)", t)
    if m:
        h = int(m.group(1) or 0)
        mnt = int(m.group(2))
        s = int(m.group(3))
        return h*3600 + mnt*60 + s
    # 00:37:54 처럼 2자리씩 고정이 아닐 수도 있어 보정
    m = re.findall(r"\d+", t)
    if len(m) == 3:  # h m s
        h, mnt, s = map(int, m)
        return h*3600 + mnt*60 + s
    if len(m) == 2:  # m s
        mnt, s = map(int, m)
        return mnt*60 + s
    return None
