#!/usr/bin/env python3
"""
일본 지진 데이터 수집 스크립트
소스: https://www.jma.go.jp/bosai/quake/data/list.json (기상청 공식 로우 데이터)
      각 지진 상세 JSON으로 위도·경도·진도 보완
실행: GitHub Actions 매시간 자동 실행
저장: data/earthquakes.json (누적, 중복 제거, 최근 365일 보존)

[2026-03-31 개선]
- cod 필드에서 lat/lon/dep 직접 파싱 → 상세 JSON 요청 횟수 대폭 감소
- dep(심도) 필드 추출 추가
- 소급 보완 대상: 좌표 없는 기존 데이터 (50건 → 100건으로 확대)
"""

import json
import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

DATA_FILE   = "data/earthquakes.json"
LIST_URL    = "https://www.jma.go.jp/bosai/quake/data/list.json"
DETAIL_BASE = "https://www.jma.go.jp/bosai/quake/data/"
KEEP_DAYS   = 365
RATE_WAIT   = 0.5   # 상세 JSON 요청 간격 (초)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (earthquake-monitor; github-actions)",
    "Referer":    "https://www.jma.go.jp/bosai/quake/",
}

# ──────────────────────────────────────────
# cod 필드 파싱: "+37.2+136.7-10000/" → lat, lon, dep
# ──────────────────────────────────────────
def parse_cod(cod_str: str) -> dict:
    """
    기상청 list.json의 cod 필드에서 위도·경도·심도를 직접 추출.
    형식: "+37.2+136.7-10000/" (심도 단위: 미터, 음수)
    반환: {"lat": "37.2", "lon": "136.7", "dep": "10"}  (dep 단위: km)
    """
    if not cod_str:
        return {}
    # 부호 포함 숫자 3개 연속 매칭
    m = re.match(r'([+-][\d.]+)([+-][\d.]+)([+-][\d]+)', cod_str)
    if not m:
        return {}
    result = {
        "lat": m.group(1).lstrip("+"),
        "lon": m.group(2).lstrip("+"),
    }
    # 심도: 미터 → km 변환 (음수이므로 절댓값)
    try:
        dep_m = abs(int(float(m.group(3))))
        if dep_m > 0:
            result["dep"] = str(dep_m // 1000)
    except (ValueError, ZeroDivisionError):
        pass
    return result


def fetch_list() -> list:
    print(f"[fetch] {LIST_URL}")
    r = requests.get(LIST_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"[fetch] {len(data)}건 수신")
    return data


def fetch_detail(json_filename: str) -> dict:
    """상세 JSON에서 lat, lon, dep, mxInt 보완 (cod 파싱으로 커버 안 되는 경우)"""
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
        eq    = d.get("earthquake", {})
        hypo  = eq.get("hypocenter", {})
        coord = hypo.get("coordinate")

        if coord:
            if isinstance(coord, dict):
                result["lat"] = str(coord.get("lat", ""))
                result["lon"] = str(coord.get("lon", ""))
                if coord.get("depth") is not None:
                    result["dep"] = str(abs(int(coord["depth"])))
            elif isinstance(coord, str):
                parsed = parse_cod(coord)
                result.update(parsed)

        # 심도 (hypocenter.depth 별도 제공하는 경우)
        if not result.get("dep") and hypo.get("depth") is not None:
            result["dep"] = str(abs(int(hypo["depth"])))

        # 최대진도
        SCALE_MAP = {
            10: "1", 20: "2", 30: "3", 40: "4",
            45: "5弱", 50: "5強", 55: "6弱", 60: "6強", 70: "7"
        }
        if eq.get("maxScale") is not None:
            result["mxInt"] = SCALE_MAP.get(eq["maxScale"], "")
        elif d.get("intensity", {}).get("maxInt"):
            result["mxInt"] = d["intensity"]["maxInt"]

        return result
    except Exception as e:
        print(f"[warn] fetch_detail 실패 ({json_filename}): {e}")
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
    """
    list.json 항목 정규화.
    1순위: cod 필드에서 직접 파싱 (lat/lon/dep)
    2순위: list.json 원본 값
    상세 JSON 요청은 이후 main()에서 필요 시에만 수행.
    """
    norm = {
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

    # ★ cod 필드에서 lat/lon/dep 직접 보완
    if norm["cod"] and (not norm["lat"] or norm["lat"] == "0"):
        parsed = parse_cod(norm["cod"])
        if parsed.get("lat"):
            norm["lat"] = parsed["lat"]
        if parsed.get("lon"):
            norm["lon"] = parsed["lon"]
        if parsed.get("dep") and not norm["dep"]:
            norm["dep"] = parsed["dep"]

    return norm


def needs_detail(item: dict) -> bool:
    """상세 JSON 요청이 필요한지 판단"""
    no_coord = not item.get("lat") or item["lat"] == "0"
    no_int   = not item.get("mxInt")
    return (no_coord or no_int) and bool(item.get("json"))


def main():
    raw_list = fetch_list()
    existing = load_existing()
    before_count = len(existing)

    # 신규 항목 필터링
    new_items = [item for item in raw_list
                 if item.get("eid") and item["eid"] not in existing]
    print(f"[new] 신규 {len(new_items)}건 처리 시작")

    detail_count = 0
    for i, item in enumerate(new_items):
        norm = normalize(item)

        # cod 파싱 후에도 좌표/진도가 없으면 상세 JSON 요청
        if needs_detail(norm):
            detail = fetch_detail(norm["json"])
            if detail.get("lat"):   norm["lat"]   = detail["lat"]
            if detail.get("lon"):   norm["lon"]   = detail["lon"]
            if detail.get("dep") and not norm["dep"]:
                                    norm["dep"]   = detail["dep"]
            if detail.get("mxInt"): norm["mxInt"] = detail["mxInt"]
            detail_count += 1
            time.sleep(RATE_WAIT)

        existing[norm["eid"]] = norm

        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(new_items)}건 처리중...")

    print(f"[merge] 기존 {before_count}건 + 신규 {len(new_items)}건 = {len(existing)}건")
    print(f"[detail] 상세 JSON 요청: {detail_count}건 (cod 파싱으로 절약)")

    # 기존 데이터 중 좌표 없는 것 소급 보완 (최근 100건)
    no_coord = [(eid, item) for eid, item in existing.items()
                if (not item.get("lat") or item["lat"] == "0") and item.get("json")]
    no_coord.sort(key=lambda x: x[1].get("at", ""), reverse=True)

    if no_coord:
        target = no_coord[:100]
        print(f"[補完] 좌표 없는 기존 데이터 {len(target)}건 소급 보완 시도")
        for eid, item in target:
            # 먼저 cod 파싱으로 시도
            if item.get("cod"):
                parsed = parse_cod(item["cod"])
                if parsed.get("lat"):
                    item["lat"] = parsed["lat"]
                    item["lon"] = parsed["lon"]
                    if parsed.get("dep") and not item.get("dep"):
                        item["dep"] = parsed["dep"]
                    continue  # 파싱 성공 → 상세 JSON 불필요

            # cod 파싱 실패 시 상세 JSON 요청
            detail = fetch_detail(item["json"])
            if detail.get("lat"):  item["lat"]   = detail["lat"]
            if detail.get("lon"):  item["lon"]   = detail["lon"]
            if detail.get("dep") and not item.get("dep"):
                                   item["dep"]   = detail["dep"]
            if detail.get("mxInt") and not item.get("mxInt"):
                                   item["mxInt"] = detail["mxInt"]
            time.sleep(RATE_WAIT)

    existing = prune_old(existing)
    save(existing)
    print("[done] 완료")


if __name__ == "__main__":
    main()
