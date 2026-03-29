#!/usr/bin/env python3
"""
일본 지진 데이터 수집 스크립트
소스: https://www.jma.go.jp/bosai/quake/data/list.json (기상청 공식 로우 데이터)
실행: GitHub Actions 매시간 자동 실행
저장: data/earthquakes.json (누적, 중복 제거, 최근 365일 보존)
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, timezone, timedelta

DATA_FILE  = "data/earthquakes.json"
LIST_URL   = "https://www.jma.go.jp/bosai/quake/data/list.json"
KEEP_DAYS  = 365   # 보존 기간 (일)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (earthquake-monitor; github-actions)",
    "Referer":    "https://www.jma.go.jp/bosai/quake/",
}

def fetch_list() -> list:
    """기상청 list.json 취득 (최근 1개월치 목록)"""
    print(f"[fetch] {LIST_URL}")
    r = requests.get(LIST_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"[fetch] {len(data)}건 수신")
    return data

def load_existing() -> dict:
    """기존 누적 데이터 로드 (eid 기준 dict)"""
    if not os.path.exists(DATA_FILE):
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        return {}
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            arr = json.load(f)
        # eid를 키로 dict 변환
        return {item["eid"]: item for item in arr if "eid" in item}
    except Exception as e:
        print(f"[warn] 기존 파일 읽기 실패: {e}")
        return {}

def save(records: dict):
    """dict → 리스트로 변환, 발생일시 내림차순 정렬 후 저장"""
    arr = sorted(records.values(), key=lambda x: x.get("at", ""), reverse=True)
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[save] {len(arr)}건 → {DATA_FILE}")

def prune_old(records: dict) -> dict:
    """KEEP_DAYS 이상 오래된 데이터 삭제"""
    cutoff = datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
    before = len(records)
    pruned = {
        eid: item for eid, item in records.items()
        if item.get("at", "") >= cutoff.strftime("%Y-%m-%dT%H:%M:%S+09:00")[:10]
    }
    removed = before - len(pruned)
    if removed:
        print(f"[prune] {removed}건 삭제 ({KEEP_DAYS}일 초과)")
    return pruned

def normalize(raw_item: dict) -> dict:
    """
    기상청 list.json 항목을 정규화
    주요 필드:
      eid    : 지진 ID (예: "20240101001234")
      at     : 발생일시 ISO8601 (예: "2024-01-01T16:10:00+09:00")
      anm    : 진원지명 (예: "石川県能登地方")
      mag    : 규모 (예: "7.6")
      mxInt  : 最大震度 문자열 (예: "7", "5弱", "5強")
      dep    : 震源深さ (예: "10")
      lat    : 緯度 (예: "37.5")
      lon    : 経度 (예: "137.2")
      cod    : 情報種別コード (예: "VXSE51", "VXSE53" 등)
      json   : 상세 JSON 파일명 (예: "20240101001234_20240101001500_VXSE53_1.json")
    """
    return {
        "eid":   raw_item.get("eid", ""),
        "at":    raw_item.get("at", ""),
        "anm":   raw_item.get("anm", ""),
        "mag":   raw_item.get("mag", ""),
        "mxInt": raw_item.get("mxInt", ""),
        "dep":   raw_item.get("dep", ""),
        "lat":   raw_item.get("lat", ""),
        "lon":   raw_item.get("lon", ""),
        "cod":   raw_item.get("cod", ""),
        "json":  raw_item.get("json", ""),
    }

def main():
    # 1. 기상청 list.json 취득
    raw_list = fetch_list()

    # 2. 기존 데이터 로드
    existing = load_existing()
    before_count = len(existing)

    # 3. 새 항목 머지 (eid 중복 제거)
    new_count = 0
    for item in raw_list:
        eid = item.get("eid")
        if not eid:
            continue
        if eid not in existing:
            existing[eid] = normalize(item)
            new_count += 1

    print(f"[merge] 기존 {before_count}건 + 신규 {new_count}건 = {len(existing)}건")

    # 4. 오래된 데이터 정리
    existing = prune_old(existing)

    # 5. 저장
    save(existing)
    print("[done] 완료")

if __name__ == "__main__":
    main()
