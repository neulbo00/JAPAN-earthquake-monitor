#!/usr/bin/env python3
"""
일본 지진 데이터 수집 스크립트
소스: https://www.jma.go.jp/bosai/quake/data/list.json (기상청 공식 로우 데이터)
      각 지진 상세 JSON으로 위도·경도·진도 보완
실행: GitHub Actions 매시간 자동 실행

[저장 구조]
data/
├── earthquakes.json        ← 최근 30일 (대시보드용, 항상 작고 빠름)
└── history/
    ├── index.json          ← 사용 가능한 월 목록 + 건수
    ├── 2026-03.json        ← 월별 아카이브 (완성된 달은 불변)
    ├── 2026-02.json
    └── ...

[변경 이력]
2026-03-31 v1: cod 필드 파싱으로 lat/lon/dep 직접 추출, 소급 보완 100건
2026-03-31 v2: 월별 아카이브 분리 (earthquakes.json = 최근 30일만)
"""

import json
import os
import re
import time
import requests
from datetime import datetime, timezone, timedelta

DATA_FILE   = "data/earthquakes.json"
HISTORY_DIR = "data/history"
LIST_URL    = "https://www.jma.go.jp/bosai/quake/data/list.json"
DETAIL_BASE = "https://www.jma.go.jp/bosai/quake/data/"
KEEP_DAYS   = 30    # earthquakes.json 보존 기간 (일) — 이전 데이터는 아카이브로 이동
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
    m = re.match(r'([+-][\d.]+)([+-][\d.]+)([+-][\d]+)', cod_str)
    if not m:
        return {}
    result = {
        "lat": m.group(1).lstrip("+"),
        "lon": m.group(2).lstrip("+"),
    }
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

        if not result.get("dep") and hypo.get("depth") is not None:
            result["dep"] = str(abs(int(hypo["depth"])))

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
    """earthquakes.json (최근 30일치) 로드"""
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
    """최근 30일치를 earthquakes.json에 저장"""
    arr = sorted(records.values(), key=lambda x: x.get("at", ""), reverse=True)
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[save] {len(arr)}건 → {DATA_FILE}")


def normalize(raw: dict) -> dict:
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
    # cod 필드에서 lat/lon/dep 직접 보완
    if norm["cod"] and (not norm["lat"] or norm["lat"] == "0"):
        parsed = parse_cod(norm["cod"])
        if parsed.get("lat"):  norm["lat"] = parsed["lat"]
        if parsed.get("lon"):  norm["lon"] = parsed["lon"]
        if parsed.get("dep") and not norm["dep"]:
                               norm["dep"] = parsed["dep"]
    return norm


def needs_detail(item: dict) -> bool:
    no_coord = not item.get("lat") or item["lat"] == "0"
    no_int   = not item.get("mxInt")
    return (no_coord or no_int) and bool(item.get("json"))


# ──────────────────────────────────────────
# 월별 아카이브
# ──────────────────────────────────────────
def archive_old(records: dict) -> dict:
    """
    30일 초과 데이터를 data/history/YYYY-MM.json으로 이동.
    - 기존 아카이브가 있으면 병합 (중복 제거)
    - index.json 업데이트
    - 반환: 최근 30일치만 남긴 dict
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")

    recent, old = {}, {}
    for eid, item in records.items():
        (recent if item.get("at", "")[:10] >= cutoff else old)[eid] = item

    if not old:
        print("[archive] 이동할 데이터 없음")
        return recent

    # 월별 그룹화
    by_month: dict[str, dict] = {}
    for eid, item in old.items():
        month = item.get("at", "")[:7]   # "YYYY-MM"
        if len(month) == 7:
            by_month.setdefault(month, {})[eid] = item

    os.makedirs(HISTORY_DIR, exist_ok=True)

    for month, items in sorted(by_month.items()):
        hist_file = os.path.join(HISTORY_DIR, f"{month}.json")

        # 기존 아카이브 로드 후 병합
        existing_hist: dict = {}
        if os.path.exists(hist_file):
            try:
                with open(hist_file, encoding="utf-8") as f:
                    arr = json.load(f)
                existing_hist = {x["eid"]: x for x in arr if "eid" in x}
            except Exception as e:
                print(f"[warn] 아카이브 읽기 실패 ({hist_file}): {e}")

        merged = {**existing_hist, **items}   # 신규가 기존을 덮어씀
        arr = sorted(merged.values(), key=lambda x: x.get("at", ""), reverse=True)

        with open(hist_file, "w", encoding="utf-8") as f:
            json.dump(arr, f, ensure_ascii=False, separators=(",", ":"))
        print(f"[archive] {month}: {len(arr)}건 → {hist_file}")

    update_index()
    print(f"[archive] 총 {len(old)}건 → {len(by_month)}개 월 파일로 이동")
    return recent


def update_index():
    """
    data/history/index.json 업데이트.
    대시보드나 외부 클라이언트가 사용 가능한 월 목록을 조회할 수 있음.
    형식: {"updated": "...", "months": [{"month": "2026-02", "count": 1234}, ...]}
    """
    os.makedirs(HISTORY_DIR, exist_ok=True)
    months = []
    for fname in os.listdir(HISTORY_DIR):
        if not fname.endswith(".json") or fname == "index.json":
            continue
        month = fname[:-5]   # "YYYY-MM"
        if len(month) != 7 or month[4] != "-":
            continue
        fpath = os.path.join(HISTORY_DIR, fname)
        try:
            with open(fpath, encoding="utf-8") as f:
                arr = json.load(f)
            count = len(arr)
        except Exception:
            count = 0
        months.append({"month": month, "count": count})

    months.sort(key=lambda x: x["month"], reverse=True)

    index = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "months":  months,
    }
    index_path = os.path.join(HISTORY_DIR, "index.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[index] {len(months)}개 월 → {index_path}")


# ──────────────────────────────────────────
# 메인
# ──────────────────────────────────────────
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

        if needs_detail(norm):
            detail = fetch_detail(norm["json"])
            if detail.get("lat"):              norm["lat"]   = detail["lat"]
            if detail.get("lon"):              norm["lon"]   = detail["lon"]
            if detail.get("dep") and not norm["dep"]:
                                               norm["dep"]   = detail["dep"]
            if detail.get("mxInt"):            norm["mxInt"] = detail["mxInt"]
            detail_count += 1
            time.sleep(RATE_WAIT)

        existing[norm["eid"]] = norm

        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(new_items)}건 처리중...")

    print(f"[merge] 기존 {before_count}건 + 신규 {len(new_items)}건 = {len(existing)}건")
    print(f"[detail] 상세 JSON 요청: {detail_count}건")

    # 좌표 없는 기존 데이터 소급 보완 (최근 100건)
    no_coord = [(eid, item) for eid, item in existing.items()
                if (not item.get("lat") or item["lat"] == "0") and item.get("json")]
    no_coord.sort(key=lambda x: x[1].get("at", ""), reverse=True)

    if no_coord:
        target = no_coord[:100]
        print(f"[補完] 좌표 없는 기존 데이터 {len(target)}건 소급 보완 시도")
        for eid, item in target:
            if item.get("cod"):
                parsed = parse_cod(item["cod"])
                if parsed.get("lat"):
                    item["lat"] = parsed["lat"]
                    item["lon"] = parsed["lon"]
                    if parsed.get("dep") and not item.get("dep"):
                        item["dep"] = parsed["dep"]
                    continue
            detail = fetch_detail(item["json"])
            if detail.get("lat"):  item["lat"]   = detail["lat"]
            if detail.get("lon"):  item["lon"]   = detail["lon"]
            if detail.get("dep") and not item.get("dep"):
                                   item["dep"]   = detail["dep"]
            if detail.get("mxInt") and not item.get("mxInt"):
                                   item["mxInt"] = detail["mxInt"]
            time.sleep(RATE_WAIT)

    # 30일 초과 데이터 → 월별 아카이브로 이동, 최근 30일만 저장
    existing = archive_old(existing)
    save(existing)
    print("[done] 완료")


if __name__ == "__main__":
    main()
