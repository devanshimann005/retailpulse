#!/usr/bin/env python3
"""
RetailPulse synthetic data generator  (Databricks Free Edition capstone)
=======================================================================

Everything produced here is SYNTHETIC. Brand, store and product names are
invented. No client data is used, which matters because Databricks Free
Edition is for non-commercial use only.

Pure Python standard library, so it runs unchanged on a laptop and in a
Databricks notebook (serverless) with no pip installs.

Two steps
---------
1. generate : build the full source set (deterministic for a given --seed)
2. release  : copy the next N simulated days of files into the landing
              zone. Run it repeatedly to simulate data arriving over time,
              so Auto Loader checkpoints and incremental logic get exercised.

Examples (laptop)
    python retail_data_generator.py generate --out ./retailpulse
    python retail_data_generator.py release  --out ./retailpulse --days 7

Examples (Databricks notebook cell; the volume must already exist)
    OUT = "/Volumes/workspace/retailpulse/raw"
    %run ./retail_data_generator   # or paste the file into a cell
    gen = Generator(seed=42); gen.generate(OUT)
    release(OUT, days=7)

Mentor only: add --key-out ./key to also write the answer key. Keep that
folder away from the engineers (do not put it inside the volume).

Layout produced under --out
    _source/            full set of files, NOT visible to the pipeline
    _source/manifest.csv   file, dataset, release_day
    landing/            what the pipeline reads; filled by `release`
        stores/ products/ sales_daily/ sales_history/ forecast_runs/ planner_overrides/
    _release_state.json progress marker for `release`
"""
import argparse
import csv
import datetime as dt
import json
import math
import os
import random
import shutil

# --------------------------------------------------------------------------
# Simulation constants
# --------------------------------------------------------------------------
FY_START = dt.date(2026, 2, 1)          # Sunday, start of fiscal 2026 (week 1)
PRIOR_FY_START = dt.date(2025, 2, 2)    # Sunday, start of fiscal 2025
SIM_WEEKS = 20                           # weeks of daily sales delivered
SIM_DAYS = SIM_WEEKS * 7                 # 140 daily files
FORECAST_HORIZON = 4                     # weeks ahead in each forecast run
N_STORES_INITIAL = 25
N_SKUS_INITIAL = 120

# Where the injected issues start. Documented for the mentor, not engineers.
PROMO_COL_FROM_DAY = 28      # promo_flag column appears (schema drift #1)
CHANNEL_COL_FROM_DAY = 63    # channel column appears   (schema drift #2)
MIXED_DATE_FILE_DAY = 35     # whole file uses dd/MM/yyyy dates
REDELIVERY_DAYS = (20, 70)   # whole file delivered a second time
STORE_SNAP2_DAY = 49         # 3 new stores, 2 re-banners
STORE_SNAP3_DAY = 98         # 1 closure, 1 region change
PRODUCT_V2_DAY = 56          # 6 new SKUs + price changes

CATEGORIES = {
    "Dairy": (0.10, 2.5, 3.0),       # category: (forecast bias, base rate, base price)
    "Bakery": (-0.04, 2.0, 2.2),
    "Beverages": (0.03, 3.2, 1.8),
    "Snacks": (0.05, 2.8, 2.0),
    "Household": (-0.02, 1.2, 6.0),
    "Personal Care": (0.00, 1.0, 5.5),
    "Frozen": (0.07, 1.6, 4.0),
    "Produce": (-0.08, 3.5, 2.8),
}
BANNERS = ["FreshMart", "ValueMax", "UrbanCart"]
REGIONS = ["North", "South", "East", "West"]
FORMATS = ["Hyper", "Super", "Express"]
CHANNELS = ["STORE", "ONLINE", "CLICK_COLLECT"]
DOW_FACTOR = [1.25, 0.85, 0.85, 0.90, 1.00, 1.15, 1.35]   # Mon..Sun order is Mon=0
LAG_SIGMA = {1: 0.12, 2: 0.16, 3: 0.20, 4: 0.25}


def iso(d):
    return d.isoformat()


def poisson(rng, lam):
    """Poisson sample without numpy. Knuth for small lam, normal approx above."""
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    limit, k, prod = math.exp(-lam), 0, 1.0
    while True:
        prod *= rng.random()
        if prod <= limit:
            return k
        k += 1


def sunday_on_or_before(d):
    # weekday(): Mon=0 ... Sun=6
    return d - dt.timedelta(days=(d.weekday() + 1) % 7)


class Generator:
    def __init__(self, seed=42):
        self.seed = seed
        self.rng = random.Random(seed)

    # ------------------------------------------------------------------
    # dimensions
    # ------------------------------------------------------------------
    def _build_stores(self):
        r = self.rng
        stores = {}
        for i in range(1, N_STORES_INITIAL + 4):
            sid = f"S{i:03d}"
            stores[sid] = dict(
                store_id=sid,
                store_name=f"Store {i:03d}",
                banner=BANNERS[r.randrange(3)],
                region=REGIONS[r.randrange(4)],
                format=FORMATS[r.randrange(3)],
                open_date=iso(dt.date(2019, 1, 1) + dt.timedelta(days=r.randrange(0, 2400))),
                status="OPEN",
                scale=round(0.6 + r.random() * 1.2, 3),
            )
        # S026-S028 open at snapshot 2
        self.new_store_open_day = STORE_SNAP2_DAY
        for sid in ("S026", "S027", "S028"):
            stores[sid]["open_date"] = iso(FY_START + dt.timedelta(days=STORE_SNAP2_DAY))
        return stores

    def _build_products(self):
        r = self.rng
        cats = list(CATEGORIES)
        prods = {}
        for i in range(1, N_SKUS_INITIAL + 7):
            sku = f"SKU{i:04d}"
            cat = cats[(i - 1) % len(cats)]
            _, base_rate, base_price = CATEGORIES[cat]
            prods[sku] = dict(
                sku=sku,
                product_name=f"{cat} Item {i:04d}",
                category=cat,
                brand=f"Brand{1 + r.randrange(12):02d}",
                pack_size=r.choice(["1ea", "6pk", "500g", "1L", "250g", "12pk"]),
                unit_price=round(base_price * (0.7 + r.random() * 1.1), 2),
                base_rate=base_rate * (0.5 + r.random() * 1.3),
                launch_day=0 if i <= N_SKUS_INITIAL else PRODUCT_V2_DAY,
            )
        # ranging: which store sells which sku (deterministic, ~60%)
        self.ranged = {}
        for sid in stores_ids(self.stores):
            for sku in prods:
                self.ranged[(sid, sku)] = r.random() < 0.6
        return prods

    # ------------------------------------------------------------------
    # demand model
    # ------------------------------------------------------------------
    def _mu(self, sid, sku, day_idx, promo):
        s, p = self.stores[sid], self.products[sku]
        d = FY_START + dt.timedelta(days=day_idx)
        # weekday(): Mon=0..Sun=6 -> DOW_FACTOR is indexed Mon..Sun
        dow = DOW_FACTOR[d.weekday()]
        season = 1.0 + 0.10 * math.sin(2 * math.pi * (day_idx + 20) / 365.0 * 1.0)
        trend = 1.0 + 0.0004 * day_idx
        promo_up = 1.6 if promo else 1.0
        return p["base_rate"] * s["scale"] * dow * season * trend * promo_up

    def _promo(self, sid, sku, day_idx):
        # promo runs in 2-week blocks for a deterministic subset
        block = day_idx // 14
        key = (sum(map(ord, sid)) * 31 + sum(map(ord, sku)) * 17 + block * 7) % 23
        return key == 0

    # ------------------------------------------------------------------
    def generate(self, out, key_out=None):
        r = self.rng
        out = os.path.abspath(out)
        src = os.path.join(out, "_source")
        if os.path.exists(src):
            shutil.rmtree(src)
        for d in ("stores", "products", "sales_daily", "sales_history", "forecast_runs", "planner_overrides"):
            os.makedirs(os.path.join(src, d), exist_ok=True)
        manifest = []      # (relative path, dataset, release_day)

        self.stores = self._build_stores()
        self.products = self._build_products()
        self.store_open_day = {sid: (STORE_SNAP2_DAY if sid in ("S026", "S027", "S028") else -1)
                               for sid in self.stores}
        self.store_last_day = {sid: SIM_DAYS + 7 * FORECAST_HORIZON for sid in self.stores}
        self.store_last_day["S004"] = STORE_SNAP3_DAY - 1   # closes at snapshot 3

        # ---- store snapshots (dimension, will be tracked as SCD type 2) -----
        snap_changes = {
            1: {},
            2: {"S010": {"banner": "UrbanCart"}, "S017": {"banner": "ValueMax"}},
            3: {"S004": {"status": "CLOSED"}, "S021": {"region": "East"}},
        }
        current = {sid: dict(s) for sid, s in self.stores.items()}
        snaps = [(1, 0, "2026-02-01"), (2, STORE_SNAP2_DAY, iso(FY_START + dt.timedelta(days=STORE_SNAP2_DAY))),
                 (3, STORE_SNAP3_DAY, iso(FY_START + dt.timedelta(days=STORE_SNAP3_DAY)))]
        self.store_snaps = []
        for n, rel_day, snap_date in snaps:
            for sid, ch in snap_changes[n].items():
                current[sid].update(ch)
            visible = [s for s in current.values()
                       if n >= 2 or int(s["store_id"][1:]) <= N_STORES_INITIAL]
            if n == 1:
                visible = [s for s in visible if int(s["store_id"][1:]) <= N_STORES_INITIAL]
            rows = [dict(store_id=s["store_id"], store_name=s["store_name"], banner=s["banner"],
                         region=s["region"], format=s["format"], open_date=s["open_date"],
                         status=s["status"], snapshot_date=snap_date) for s in visible]
            rel = f"stores/stores_snapshot_{snap_date.replace('-', '')}.csv"
            self._write_csv(os.path.join(src, rel), rows)
            manifest.append((rel, "stores", rel_day))
            self.store_snaps.append((n, snap_date, rows))

        # ---- products ---------------------------------------------------
        def prod_rows(version):
            rows = []
            for p in self.products.values():
                if version == 1 and p["launch_day"] > 0:
                    continue
                price = p["unit_price"]
                if version == 2 and int(p["sku"][3:]) % 12 == 0:
                    price = round(price * 1.08, 2)          # 10 price changes
                rows.append(dict(sku=p["sku"], product_name=p["product_name"], category=p["category"],
                                 brand=p["brand"], pack_size=p["pack_size"], unit_price=price))
            return rows
        self.price_v2 = {x["sku"]: x["unit_price"] for x in prod_rows(2)}
        self.price_v1 = {x["sku"]: x["unit_price"] for x in prod_rows(1)}
        for v, rel_day in ((1, 0), (2, PRODUCT_V2_DAY)):
            rel = f"products/products_v{v}.csv"
            self._write_csv(os.path.join(src, rel), prod_rows(v))
            manifest.append((rel, "products", rel_day))

        # ---- daily sales ------------------------------------------------
        self._build_sales(src, manifest)
        # ---- history (prior fiscal year, weekly grain) ------------------
        self._build_history(src, manifest)
        # ---- forecasts + overrides --------------------------------------
        self._build_forecasts(src, manifest)

        with open(os.path.join(src, "manifest.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["path", "dataset", "release_day"])
            for row in sorted(manifest, key=lambda x: (x[2], x[0])):
                w.writerow(row)
        for d in ("stores", "products", "sales_daily", "sales_history", "forecast_runs", "planner_overrides"):
            os.makedirs(os.path.join(out, "landing", d), exist_ok=True)
        with open(os.path.join(out, "_release_state.json"), "w") as f:
            json.dump({"released_through_day": -1}, f)

        if key_out:
            self._write_key(os.path.abspath(key_out))
        print(f"Generated {len(manifest)} source files under {src}")
        return manifest

    # ------------------------------------------------------------------
    def _build_sales(self, src, manifest):
        r = self.rng
        sku_list = list(self.products)
        store_ids = stores_ids(self.stores)

        # true (clean) records by day
        true_rows = {}
        self.weekly_actual = {}     # (sid, sku, week_idx) -> units (clean, restated-final)
        for day in range(SIM_DAYS):
            date = FY_START + dt.timedelta(days=day)
            rows = []
            for sid in store_ids:
                if day <= self.store_open_day[sid] - 1 and self.store_open_day[sid] >= 0:
                    continue
                if day > self.store_last_day[sid]:
                    continue
                for sku in sku_list:
                    p = self.products[sku]
                    if day < p["launch_day"] or not self.ranged[(sid, sku)]:
                        continue
                    promo = self._promo(sid, sku, day)
                    q = poisson(r, self._mu(sid, sku, day, promo))
                    if q <= 0:
                        continue
                    price = self.price_v2[sku] if (day >= PRODUCT_V2_DAY and sku in self.price_v2) else \
                        self.price_v1.get(sku, self.price_v2.get(sku))
                    ts = f"{iso(date)}T23:{r.randrange(0, 60):02d}:{r.randrange(0, 60):02d}"
                    rows.append(dict(sale_id=f"{sid}-{sku}-{iso(date)}", store_id=sid, sku=sku,
                                     sale_date=iso(date), quantity=q, unit_price=price,
                                     event_ts=ts, promo_flag=("Y" if promo else "N"),
                                     channel=CHANNELS[r.randrange(3)]))
            true_rows[day] = rows

        # --- defect planning (each row gets at most ONE value defect) ------
        self.defects = {"NULL_KEY": 0, "NEGATIVE_QTY": 0, "ORPHAN_STORE": 0, "STRING_QTY": 0,
                        "MESSY_SKU": 0, "RESTATED": 0, "LATE_ARRIVAL": 0, "INTRAFILE_DUP": 0,
                        "CORRUPT_LINE": 0, "REDELIVERED_FILES": 0, "MIXED_DATE_ROWS": 0}
        delivered = {d: [] for d in range(SIM_DAYS + 8)}      # file index -> list of row dicts or raw strings
        self.valid_final = {}        # sale_id -> final valid row (after restatement)
        self.quarantine = {"NULL_KEY": set(), "NEGATIVE_QTY": set(), "ORPHAN_STORE": set()}
        corrupt_budget = 5

        for day in range(SIM_DAYS):
            for row in true_rows[day]:
                row = dict(row)
                roll = r.random()
                defect = None
                if roll < 0.0010:
                    defect = "NULL_KEY"
                elif roll < 0.0020:
                    defect = "NEGATIVE_QTY"
                elif roll < 0.0030:
                    defect = "ORPHAN_STORE"
                elif roll < 0.0050:
                    defect = "STRING_QTY"
                elif roll < 0.0100:
                    defect = "MESSY_SKU"

                orig_sku = row["sku"]
                if defect == "NULL_KEY":
                    # null either store or sku, alternate deterministically
                    if r.random() < 0.5:
                        row["store_id"] = None
                    else:
                        row["sku"] = None
                    self.quarantine["NULL_KEY"].add(row["sale_id"])
                elif defect == "NEGATIVE_QTY":
                    row["quantity"] = -row["quantity"]
                    self.quarantine["NEGATIVE_QTY"].add(row["sale_id"])
                elif defect == "ORPHAN_STORE":
                    row["store_id"] = "S999"
                    self.quarantine["ORPHAN_STORE"].add(row["sale_id"])
                elif defect == "STRING_QTY":
                    row["quantity"] = str(row["quantity"])
                elif defect == "MESSY_SKU":
                    row["sku"] = " " + orig_sku.lower() + " " if r.random() < 0.5 else orig_sku.lower()
                if defect:
                    self.defects[defect] += 1

                valid = defect in (None, "STRING_QTY", "MESSY_SKU")
                if valid:
                    clean = dict(row)
                    clean["sku"] = orig_sku
                    clean["quantity"] = int(clean["quantity"])
                    self.valid_final[row["sale_id"]] = clean

                # late arrival: file of day+1..day+5 instead of day+1
                file_idx = day + 1
                if r.random() < 0.010 and day + 5 < SIM_DAYS + 1:
                    file_idx = day + 1 + r.randrange(1, 5)
                    self.defects["LATE_ARRIVAL"] += 1
                delivered[file_idx].append(row)

                # restatement: same sale_id, later file, different quantity, later event_ts
                if valid and r.random() < 0.003 and file_idx + 3 <= SIM_DAYS:
                    rest = dict(row)
                    base_q = int(rest["quantity"])
                    new_q = max(1, base_q + r.choice([-2, -1, 1, 2, 3]))
                    rest["quantity"] = new_q
                    rest["event_ts"] = f"{iso(FY_START + dt.timedelta(days=file_idx + 2))}T06:00:00"
                    delivered[file_idx + 2].append(rest)
                    self.valid_final[row["sale_id"]]["quantity"] = new_q
                    self.valid_final[row["sale_id"]]["event_ts"] = rest["event_ts"]
                    self.defects["RESTATED"] += 1

                # intra-file exact duplicate
                if r.random() < 0.003:
                    delivered[file_idx].append(dict(row))
                    self.defects["INTRAFILE_DUP"] += 1

        # --- write one file per day, with schema drift, mixed dates, corruption ---
        id_count = {}
        for rows_ in delivered.values():
            for row_ in rows_:
                id_count[row_["sale_id"]] = id_count.get(row_["sale_id"], 0) + 1
        redeliver_idx = set(REDELIVERY_DAYS)
        corrupt_files = {15, 42, 77, 101, 120}
        for fi in range(1, SIM_DAYS + 1):
            rows = delivered[fi]
            if not rows:
                continue
            date_of_file = FY_START + dt.timedelta(days=fi - 1)
            fname = f"sales_{iso(date_of_file).replace('-', '')}.json"
            rel = f"sales_daily/{fname}"
            lines = []
            for row in rows:
                rec = {k: row[k] for k in ("sale_id", "store_id", "sku", "sale_date", "quantity", "unit_price", "event_ts")}
                if fi - 1 >= PROMO_COL_FROM_DAY:
                    rec["promo_flag"] = row["promo_flag"]
                if fi - 1 >= CHANNEL_COL_FROM_DAY:
                    rec["channel"] = row["channel"]
                if fi - 1 == MIXED_DATE_FILE_DAY:
                    y, m, d = rec["sale_date"].split("-")
                    rec["sale_date"] = f"{d}/{m}/{y}"
                    self.defects["MIXED_DATE_ROWS"] += 1
                lines.append(json.dumps(rec))
            if fi - 1 in corrupt_files:
                # truncate one line mid-record (unparseable JSON). Pick a row whose sale_id is
                # delivered exactly once, so the loss is total and the answer key stays simple.
                cands = [i for i, row in enumerate(rows) if id_count[row["sale_id"]] == 1]
                k = cands[r.randrange(len(cands))]
                lines[k] = lines[k][: len(lines[k]) // 2]
                self.defects["CORRUPT_LINE"] += 1
                lost = rows[k]["sale_id"]
                if lost in self.valid_final:
                    del self.valid_final[lost]
                    self.defects["LOST_TO_CORRUPTION"] = self.defects.get("LOST_TO_CORRUPTION", 0) + 1
                for reason in self.quarantine:
                    self.quarantine[reason].discard(lost)
            with open(os.path.join(src, rel), "w") as f:
                f.write("\n".join(lines) + "\n")
            manifest.append((rel, "sales_daily", fi))
            if (fi - 1) in redeliver_idx:
                rel2 = f"sales_daily/sales_{iso(date_of_file).replace('-', '')}_redelivery.json"
                shutil.copyfile(os.path.join(src, rel), os.path.join(src, rel2))
                manifest.append((rel2, "sales_daily", fi + 1))
                self.defects["REDELIVERED_FILES"] += 1

    # ------------------------------------------------------------------
    def _week_mu(self, sid, sku, week_idx):
        total = 0.0
        for d in range(7):
            day = week_idx * 7 + d
            p = self.products[sku]
            if day < p["launch_day"]:
                continue
            if self.store_open_day[sid] >= 0 and day < self.store_open_day[sid]:
                continue
            if day > self.store_last_day[sid]:
                continue
            total += self._mu(sid, sku, day, self._promo(sid, sku, day))
        return total

    def _build_history(self, src, manifest):
        """Prior fiscal year (FY2025) at store x sku x week grain, one file."""
        r = self.rng
        rows = []
        for w in range(52):
            wk_start = PRIOR_FY_START + dt.timedelta(days=7 * w)
            for sid in stores_ids(self.stores):
                if int(sid[1:]) > N_STORES_INITIAL:
                    continue
                for sku, p in self.products.items():
                    if p["launch_day"] > 0 or not self.ranged[(sid, sku)]:
                        continue
                    # last year ran a few % below this year with its own seasonality
                    day_equiv = w * 7 + 3
                    season = 1.0 + 0.10 * math.sin(2 * math.pi * (day_equiv + 20) / 365.0)
                    mu = p["base_rate"] * self.stores[sid]["scale"] * 7 * season * 1.03
                    units = poisson(r, mu)
                    rows.append(dict(fiscal_year=2025, fiscal_week=w + 1, week_start=iso(wk_start),
                                     store_id=sid, sku=sku, units=units))
        rel = "sales_history/sales_history_fy2025.csv"
        self._write_csv(os.path.join(src, rel), rows)
        manifest.append((rel, "sales_history", 0))
        self.history = rows

    def _build_forecasts(self, src, manifest):
        r = self.rng
        # actual weekly units, computed from the FINAL valid sales (what the business "really" sold)
        wk_actual = {}
        for rec in self.valid_final.values():
            day = (dt.date.fromisoformat(rec["sale_date"]) - FY_START).days
            wk_actual[(rec["store_id"], rec["sku"], day // 7)] = \
                wk_actual.get((rec["store_id"], rec["sku"], day // 7), 0) + rec["quantity"]
        self.weekly_actual = wk_actual

        first_run = FY_START - dt.timedelta(days=28)         # 2026-01-04
        n_runs = SIM_WEEKS + 4                                # last run = start of week 20 (index 19) + ...
        self.forecast_rows = []
        self.override_rows = []
        for run_i in range(n_runs):
            run_date = first_run + dt.timedelta(days=7 * run_i)
            rel_day = max(0, (run_date - FY_START).days)
            run_rows, ov_rows = [], []
            for lag in range(1, FORECAST_HORIZON + 1):
                target_start = run_date + dt.timedelta(days=7 * lag)
                week_idx = (target_start - FY_START).days // 7        # 0 = FY week 1
                if week_idx < 0:
                    continue
                for sid in stores_ids(self.stores):
                    for sku in self.products:
                        if not self.ranged[(sid, sku)]:
                            continue
                        mu = self._week_mu(sid, sku, week_idx)
                        if mu <= 0.25:
                            continue
                        cat = self.products[sku]["category"]
                        bias = CATEGORIES[cat][0]
                        f = mu * math.exp(bias + LAG_SIGMA[lag] * r.gauss(0, 1))
                        f = max(0, int(round(f)))
                        run_rows.append(dict(forecast_run_date=iso(run_date), target_week_start=iso(target_start),
                                             lag_weeks=lag, store_id=sid, sku=sku, forecast_units=f))
                        # planner override (about 1.5% of rows)
                        if r.random() < 0.015:
                            if r.random() < 0.75:
                                newv = max(0, int(round(mu * math.exp(r.gauss(0, 0.08)))))
                            else:
                                newv = max(0, int(round(f * 1.6)))      # a bad override
                            if newv != f:
                                hour = 9 + r.randrange(0, 6)
                                ov_rows.append(dict(override_id=f"OV-{run_date:%Y%m%d}-{len(ov_rows) + 1:05d}",
                                                    forecast_run_date=iso(run_date),
                                                    target_week_start=iso(target_start),
                                                    store_id=sid, sku=sku, old_units=f, new_units=newv,
                                                    planner=f"planner{1 + r.randrange(4)}",
                                                    reason=r.choice(["promo", "supply", "local_event", "gut_feel"]),
                                                    created_ts=f"{iso(run_date)}T{hour:02d}:{r.randrange(0, 60):02d}:00"))
                                # ~12%: the planner revisits the same cell later the same day -> latest wins
                                if r.random() < 0.12:
                                    newer = max(0, int(round(mu * math.exp(r.gauss(0, 0.05)))))
                                    if newer != newv:
                                        ov_rows.append(dict(override_id=f"OV-{run_date:%Y%m%d}-{len(ov_rows) + 1:05d}",
                                                            forecast_run_date=iso(run_date),
                                                            target_week_start=iso(target_start),
                                                            store_id=sid, sku=sku, old_units=newv, new_units=newer,
                                                            planner=ov_rows[-1]["planner"], reason="revision",
                                                            created_ts=f"{iso(run_date)}T{hour + 1 + r.randrange(0, 2):02d}:{r.randrange(0, 60):02d}:00"))
            rel = f"forecast_runs/forecast_run_{run_date:%Y%m%d}.csv"
            self._write_csv(os.path.join(src, rel), run_rows)
            manifest.append((rel, "forecast_runs", rel_day))
            self.forecast_rows.extend(run_rows)
            if ov_rows:
                relo = f"planner_overrides/overrides_{run_date:%Y%m%d}.json"
                with open(os.path.join(src, relo), "w") as f:
                    f.write("\n".join(json.dumps(o) for o in ov_rows) + "\n")
                manifest.append((relo, "planner_overrides", rel_day))
                self.override_rows.extend(ov_rows)

    # ------------------------------------------------------------------
    def _write_csv(self, path, rows):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not rows:
            open(path, "w").close()
            return
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    # ------------------------------------------------------------------
    def _fiscal_calendar_rows(self):
        """4-5-4 calendar: weeks 1-4 = period 1, 5-9 = period 2, 10-13 = period 3, repeating,
        quarters of 13 weeks. FY starts on the Sunday on/after 1 Feb (hard-coded for FY2025/FY2026)."""
        rows = []
        for fy, start in ((2025, PRIOR_FY_START), (2026, FY_START)):
            pattern = [4, 5, 4] * 4
            wk = 0
            for per, nweeks in enumerate(pattern, start=1):
                for _ in range(nweeks):
                    wk += 1
                    for d in range(7):
                        date = start + dt.timedelta(days=7 * (wk - 1) + d)
                        rows.append(dict(date=iso(date), fiscal_year=fy, fiscal_quarter=(per - 1) // 3 + 1,
                                         fiscal_period=per, fiscal_week=wk, day_of_week=date.isoweekday() % 7 + 1))
        return rows

    def _write_key(self, key_out):
        os.makedirs(key_out, exist_ok=True)
        qsum = {k: len(v) for k, v in self.quarantine.items()}
        total_units = sum(v["quantity"] for v in self.valid_final.values())
        by_week = {}
        for v in self.valid_final.values():
            day = (dt.date.fromisoformat(v["sale_date"]) - FY_START).days
            by_week[day // 7 + 1] = by_week.get(day // 7 + 1, 0) + v["quantity"]
        # forecast accuracy answer key (system forecast only, lag 1 and lag 4), whole network
        acc = {}
        for lag in (1, 4):
            num = den = 0
            for fr in self.forecast_rows:
                if fr["lag_weeks"] != lag:
                    continue
                wi = (dt.date.fromisoformat(fr["target_week_start"]) - FY_START).days // 7
                if wi >= SIM_WEEKS:
                    continue
                actual = self.weekly_actual.get((fr["store_id"], fr["sku"], wi), 0)
                num += abs(fr["forecast_units"] - actual)
                den += actual
            acc[f"system_wape_lag{lag}"] = round(num / den, 4) if den else None
        latest = {}
        for o in self.override_rows:
            k = (o["forecast_run_date"], o["target_week_start"], o["store_id"], o["sku"])
            if k not in latest or o["created_ts"] > latest[k]["created_ts"]:
                latest[k] = o
        for lag in (1, 4):
            num = den = 0
            for fr in self.forecast_rows:
                if fr["lag_weeks"] != lag:
                    continue
                wi = (dt.date.fromisoformat(fr["target_week_start"]) - FY_START).days // 7
                if wi >= SIM_WEEKS:
                    continue
                k = (fr["forecast_run_date"], fr["target_week_start"], fr["store_id"], fr["sku"])
                final = latest[k]["new_units"] if k in latest else fr["forecast_units"]
                actual = self.weekly_actual.get((fr["store_id"], fr["sku"], wi), 0)
                num += abs(final - actual)
                den += actual
            acc[f"final_wape_lag{lag}"] = round(num / den, 4) if den else None
        hist = {(h["store_id"], h["sku"], h["fiscal_week"]): h["units"] for h in self.history}
        comp_keys = {(a, b) for (a, b, _w) in hist}
        lfl = {}
        for w in range(1, SIM_WEEKS + 1):
            cur = sum(u for (sid, sku, wi), u in self.weekly_actual.items() if wi == w - 1 and (sid, sku) in comp_keys)
            prev = sum(u for (sid, sku, fw), u in hist.items() if fw == w)
            lfl[w] = round(cur / prev - 1, 4) if prev else None
        acc["like_for_like_yoy_by_fiscal_week"] = lfl
        expected = dict(
            seed=self.seed,
            silver_sales_rows=len(self.valid_final),
            silver_sales_total_units=total_units,
            quarantine_rows=qsum,
            quarantine_total=sum(qsum.values()),
            injected_defects=self.defects,
            units_by_fiscal_week=dict(sorted(by_week.items())),
            forecast_rows_total=len(self.forecast_rows),
            override_rows_total=len(self.override_rows),
            **acc,
        )
        with open(os.path.join(key_out, "expected.json"), "w") as f:
            json.dump(expected, f, indent=2)
        self._write_csv(os.path.join(key_out, "fiscal_calendar.csv"), self._fiscal_calendar_rows())
        # quarantine ids for spot-checking
        with open(os.path.join(key_out, "quarantine_sale_ids.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["sale_id", "reason"])
            for reason, ids in self.quarantine.items():
                for sid in sorted(ids):
                    w.writerow([sid, reason])
        # silver truth for diffing
        with open(os.path.join(key_out, "silver_sales_truth.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["sale_id", "store_id", "sku", "sale_date", "quantity", "unit_price"])
            for sid in sorted(self.valid_final):
                v = self.valid_final[sid]
                w.writerow([sid, v["store_id"], v["sku"], v["sale_date"], v["quantity"], v["unit_price"]])
        print(f"Answer key written to {key_out}")


def stores_ids(stores):
    return sorted(stores)


# --------------------------------------------------------------------------
# release: copy the next N simulated days of files into landing/
# --------------------------------------------------------------------------
def release(out, days=1, through_day=None):
    out = os.path.abspath(out)
    state_path = os.path.join(out, "_release_state.json")
    with open(state_path) as f:
        state = json.load(f)
    start = state["released_through_day"]
    target = through_day if through_day is not None else start + days
    target = min(target, SIM_DAYS + 1)
    copied = 0
    with open(os.path.join(out, "_source", "manifest.csv")) as f:
        for row in csv.DictReader(f):
            rd = int(row["release_day"])
            if start < rd <= target or (start == -1 and rd <= target):
                dst = os.path.join(out, "landing", row["path"])
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copyfile(os.path.join(out, "_source", row["path"]), dst)
                copied += 1
    state["released_through_day"] = target
    with open(state_path, "w") as f:
        json.dump(state, f)
    d = FY_START + dt.timedelta(days=max(target - 1, 0))
    print(f"Released through simulated day {target} (data up to {d}); copied {copied} files")
    return copied


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--out", required=True)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--key-out", default=None, help="mentor only: write the answer key here")
    r = sub.add_parser("release")
    r.add_argument("--out", required=True)
    r.add_argument("--days", type=int, default=1)
    r.add_argument("--through-day", type=int, default=None)
    a = ap.parse_args()
    if a.cmd == "generate":
        Generator(seed=a.seed).generate(a.out, a.key_out)
    else:
        release(a.out, a.days, a.through_day)


if __name__ == "__main__":
    main()
