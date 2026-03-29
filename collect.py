#!/usr/bin/env python3
"""
일본 지진 데이터 수집 스크립트
소스: https://www.jma.go.jp/bosai/quake/data/list.json (기상청 공식 로우 데이터)
      각 지진 상세 JSON으로 위도·경도·진도 보완
실행: GitHub Actions 매시간 자동 실행
저장: data/earthquakes.json (누적, 중복 제거, 최근 365일 보존)
"""

import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta

DATA_FILE  = "data/earthquakes.json"
LIST_URL   = "https://www.jma.go.jp/bosai/quake/data/list.json"
DETAIL_BASE= "https://www.jma.go.jp/bosai/quake/data/"
KEEP_DAYS  = 365
RATE_WAIT  = 0.5   # 상세 JSON 요청 간격 (초)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (earthquake-monitor; github-actions)",
    "Referer":    "https://www.jma.go.jp/bosai/quake/",
}

def fetch_list() -> list:
    print(f"[fetch] {LIST_URL}")
    r = requests.get(LIST_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"[fetch] {len(data)}건 수신")
    return data

def fetch_detail(json_filename: str) -> dict:
    """상세 JSON에서 lat, lon, maxInt 보완"""
    if not json_filename:
        return {}
    url = DETAIL_BASE + json_filename
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return {}
        d = r.json()
        result = {}
        # 진원 정보
        eq = d.get("earthquake", {})
        hypo = eq.get("hypocenter", {})
        if hypo.get("coordinate"):
            coord = hypo["coordinate"]
            # 좌표 형식: "+37.7+137.3-10/" 또는 {"lat":..., "lon":...}
            if isinstance(coord, dict):
                result["lat"] = str(coord.get("lat", ""))
                result["lon"] = str(coord.get("lon", ""))
            elif isinstance(coord, str):
                import re
                m = re.match(r'([+-][\d.]+)([+-][\d.]+)', coord)
                if m:
                    result["lat"] = m.group(1)
                    result["lon"] = m.group(2)
        # 최대진도
        if eq.get("maxScale") is not None:
            scale_map = {
                10:"1", 20:"2", 30:"3", 40:"4",
                45:"5弱", 50:"5強", 55:"6弱", 60:"6強", 70:"7"
            }
            result["mxInt"] = scale_map.get(eq["maxScale"], "")
        elif d.get("intensity", {}).get("maxInt"):
            result["mxInt"] = d["intensity"]["maxInt"]
        return result
    except Exception as e:
        return {}

def load_existing() -> dict:
    if not os.path.exists(DATA_FILE):
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        return {}
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            arr = json.load(f)
        return {item["eid"]: item for item in arr if "eid" in item}
    except Exception as e:
        print(f"[warn] 기존 파일 읽기 실패: {e}")
        return {}

def save(records: dict):
    arr = sorted(records.values(), key=lambda x: x.get("at", ""), reverse=True)
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[save] {len(arr)}건 → {DATA_FILE}")

def prune_old(records: dict) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    pruned = {eid: item for eid, item in records.items()
              if item.get("at", "")[:10] >= cutoff}
    removed = len(records) - len(pruned)
    if removed:
        print(f"[prune] {removed}건 삭제 ({KEEP_DAYS}일 초과)")
    return pruned

def normalize(raw: dict) -> dict:
    return {
        "eid":   raw.get("eid", ""),
        "at":    raw.get("at", ""),
        "anm":   raw.get("anm", ""),
        "mag":   raw.get("mag", ""),
        "mxInt": raw.get("mxInt", ""),
        "dep":   raw.get("dep", ""),
        "lat":   raw.get("lat", ""),
        "lon":   raw.get("lon", ""),
        "cod":   raw.get("cod", ""),
        "json":  raw.get("json", ""),
    }

def main():
    raw_list = fetch_list()
    existing = load_existing()
    before_count = len(existing)

    new_items = []
    for item in raw_list:
        eid = item.get("eid")
        if not eid or eid in existing:
            continue
        new_items.append(item)

    print(f"[new] 신규 {len(new_items)}건 상세 데이터 수집 시작")

    # 신규 항목만 상세 JSON 요청해서 lat/lon/mxInt 보완
    for i, item in enumerate(new_items):
        norm = normalize(item)

        # list.json에 lat/lon/mxInt 없으면 상세 JSON에서 보완
        needs_detail = (not norm["lat"] or norm["lat"] == "0") or not norm["mxInt"]
        if needs_detail and norm["json"]:
            detail = fetch_detail(norm["json"])
            if detail.get("lat"):  norm["lat"]   = detail["lat"]
            if detail.get("lon"):  norm["lon"]   = detail["lon"]
            if detail.get("mxInt"): norm["mxInt"] = detail["mxInt"]
            time.sleep(RATE_WAIT)

        existing[norm["eid"]] = norm

        if (i+1) % 10 == 0:
            print(f"  {i+1}/{len(new_items)}건 처리중...")

    print(f"[merge] 기존 {before_count}건 + 신규 {len(new_items)}건 = {len(existing)}건")

    # 기존 데이터 중 lat/lon 없는 것도 소급 보완 (최근 100건만)
    no_coord = [(eid, item) for eid, item in existing.items()
                if (not item.get("lat") or item["lat"] == "0") and item.get("json")]
    no_coord.sort(key=lambda x: x[1].get("at",""), reverse=True)
    if no_coord:
        print(f"[補完] 좌표 없는 기존 데이터 {min(len(no_coord),50)}건 보완 시도")
        for eid, item in no_coord[:50]:
            detail = fetch_detail(item["json"])
            if detail.get("lat"): item["lat"] = detail["lat"]
            if detail.get("lon"): item["lon"] = detail["lon"]
            if detail.get("mxInt") and not item.get("mxInt"): item["mxInt"] = detail["mxInt"]
            time.sleep(RATE_WAIT)

    existing = prune_old(existing)
    save(existing)
    print("[done] 완료")

if __name__ == "__main__":
    main()
