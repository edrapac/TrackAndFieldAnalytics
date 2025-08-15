import csv, io, re, os
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any, Optional

in_path = "/mnt/data/Mens_200m_by_athlete.csv"
out_path = "/mnt/data/Mens_200m_splits_tidy.csv"

# ---------------- Parser (token-based, robust to shifting columns) ----------------

def norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    # normalize NBSP/zero-widths/quotes
    s = s.replace("\u00A0"," ").replace("\u200B","")
    s = s.strip().strip('"')
    return s

def parse_float(x: Optional[str]) -> Optional[float]:
    s = norm(x)
    if s == "" or s.upper() in {"NA","NULL"}:
        return None
    s = re.sub(r"[^0-9.\-]+", "", s)  # keep digits, dot, minus
    if s in {"","-","--"}:
        return None
    try:
        return float(s)
    except Exception:
        return None

def looks_like_athlete(cell: str) -> bool:
    # "Lastname, Firstname (XXX) (YYYY)" pattern (country + birth year)
    c = norm(cell)
    return bool(re.search(r"\([A-Z]{3}\)\s*\(\d{4}\)", c))

def is_source_text(txt: str) -> bool:
    return any(k in txt for k in ["Timing", "www.", "Tsuchie", "analysis", "run speed"])

def row_contains_token(row: List[str], token: str) -> bool:
    tl = token.lower()
    return any(norm(c).lower() == tl for c in row)

def find_token_idx(row: List[str], token: str) -> Optional[int]:
    tl = token.lower()
    for i, c in enumerate(row):
        if norm(c).lower() == tl:
            return i
    return None

def first_nonempty_after(row: List[str], start_idx: int) -> Optional[str]:
    for j in range(start_idx+1, len(row)):
        if norm(row[j]) != "":
            return row[j]
    return None

def last_nonempty(row: List[str]) -> Optional[str]:
    for c in reversed(row):
        if norm(c) != "":
            return c
    return None

def is_meet_line(row: List[str]) -> bool:
    # Typical: first cell empty, second has meet text
    if len(row) < 2: return False
    c0, c1 = norm(row[0]), norm(row[1])
    # Be permissive: treat as meet if first is empty and second non-empty and looks like a meet string
    looks_meetish = (" - " in c1) or ("(" in c1 and ")" in c1)
    return (c0 == "" and c1 != "" and looks_meetish)

def compute_derived(p: Dict[str, Any]) -> None:
    t100 = p.get("100m")
    t200 = p.get("200m")
    if t100 is not None:
        p["0-100m"] = round(t100, 2)
        p["Vel_0_100m"] = round(100.0 / t100, 2) if t100 > 0 else None
    if t100 is not None and t200 is not None:
        seg2 = t200 - t100
        p["100-200m"] = round(seg2, 2)
        p["Vel_100_200m"] = round(100.0 / seg2, 2) if seg2 > 0 else None
        p["Differential"] = round(seg2 - t100, 2)

# Read file
with open(in_path, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.reader(f)
    rows = [[norm(c) for c in r] for r in reader]

records: List[Dict[str, Any]] = []
current_athlete = None
current_meet = None
current_source = None
pending: Dict[str, Any] = {}

def flush():
    global pending
    if not pending:
        return
    compute_derived(pending)
    sr = pending.get("StrideRate")
    pending["Ath_Mt_Strd"] = f'{pending.get("Athlete","")}_{pending.get("Meet Info","")}_{sr if sr is not None else ""}'.strip("_")
    if current_source and not pending.get("Source"):
        pending["Source"] = current_source
    records.append(pending)
    pending = {}

for row in rows:
    # remove trailing empties
    while row and row[-1] == "":
        row.pop()
    if not row:
        continue

    # Athlete header
    if looks_like_athlete(row[0]):
        flush()
        current_athlete = row[0]
        current_meet = None
        current_source = None
        continue

    # Meet line
    if is_meet_line(row):
        # meet info is first nonempty after col0
        for c in row[1:]:
            if norm(c) != "":
                current_meet = norm(c)
                break
        tail = last_nonempty(row[2:]) if len(row) > 2 else None
        if tail and is_source_text(tail):
            current_source = tail
        continue

    # Standalone source
    joined = ",".join(row)
    if is_source_text(joined) and not row_contains_token(row, "date"):
        maybe = last_nonempty(row)
        if maybe:
            current_source = maybe
        continue

    # Date/Time row (start of a performance)
    d_idx = find_token_idx(row, "date")
    if d_idx is not None:
        flush()
        t_idx = find_token_idx(row, "time")
        # Lane/Place search (first cell after 'time' containing a slash)
        lane_place = None
        if t_idx is not None:
            for c in row[t_idx+1:]:
                if "/" in c:
                    lane_place = c
                    break
        # Pull splits after 'time' if possible
        splits = {"50m": None, "100m": None, "150m": None, "200m": None}
        time_val = None
        # Strategy A: after 'time', next 5 numeric tokens are 50/100/150/200 + Official Time
        if t_idx is not None:
            nums = [parse_float(x) for x in row[t_idx+1:]]
            nums = [x for x in nums if x is not None]
            if len(nums) >= 5:
                splits["50m"], splits["100m"], splits["150m"], splits["200m"], time_val = nums[:5]
            elif len(nums) >= 4:
                splits["50m"], splits["100m"], splits["150m"], splits["200m"] = nums[:4]
                time_val = splits["200m"]
        # Strategy B: scan all numeric tokens if strategy A missing a value
        if splits["50m"] is None:
            nums = [parse_float(x) for x in row]
            nums = [x for x in nums if x is not None]
            if len(nums) >= 4:
                splits["50m"], splits["100m"], splits["150m"], splits["200m"] = nums[:4]
                time_val = nums[4] if len(nums) > 4 else splits["200m"]

        # Compose pending
        pending = {
            "Athlete": current_athlete,
            "Meet Info": current_meet,
            "Date": first_nonempty_after(row, d_idx),
            "50m": splits["50m"],
            "100m": splits["100m"],
            "150m": splits["150m"],
            "200m": splits["200m"],
            "Time": time_val if time_val is not None else splits["200m"],
            "Lane_Place": lane_place,
            "Source": current_source,
        }
        continue

    # Reaction time row
    rt_idx = find_token_idx(row, "reaction time")
    if rt_idx is not None:
        rt_val = first_nonempty_after(row, rt_idx)
        if rt_val:
            pending["RT"] = parse_float(rt_val)
        continue

    # Wind and velocities
    w_idx = find_token_idx(row, "wind")
    if w_idx is not None:
        w_val = first_nonempty_after(row, w_idx)
        if w_val:
            pending["Wind"] = w_val
        v_idx = find_token_idx(row, "velocity")
        if v_idx is not None:
            # Next up to 4 numbers may be velocities at 50/100/150/200
            vel_vals = []
            for c in row[v_idx+1:v_idx+10]:
                f = parse_float(c)
                if f is not None:
                    vel_vals.append(f)
            if len(vel_vals) >= 1: pending["Vel_50m"] = vel_vals[0] if len(vel_vals) > 0 else None
            if len(vel_vals) >= 2: pending["Vel_100m"] = vel_vals[1]
            if len(vel_vals) >= 3: pending["Vel_150m"] = vel_vals[2]
            if len(vel_vals) >= 4: pending["Vel_200m"] = vel_vals[3]
            # stride rate detection: look for an integer 20..130 after 'velocity'
            for c in row[v_idx+1:]:
                sc = norm(c)
                if sc.isdigit():
                    iv = int(sc)
                    if 20 <= iv <= 130:
                        pending["StrideRate"] = iv
                        break
        continue

# Flush last performance
flush()

# Build DataFrame with consistent column order
cols = [
    "Athlete", "Meet Info", "Lane_Place", "Wind", "Date", "RT",
    "50m", "100m", "150m", "200m", "Time",
    "0-100m", "100-200m", "Differential",
    "Vel_50m", "Vel_100m", "Vel_150m", "Vel_200m",
    "Vel_0_100m", "Vel_100_200m",
    "StrideRate", "Source", "Ath_Mt_Strd"
]
df = pd.DataFrame(records)
for c in cols:
    if c not in df.columns:
        df[c] = None
df = df[cols]

# Save output
df.to_csv(out_path, index=False)
print(f"Saved: {out_path}")
