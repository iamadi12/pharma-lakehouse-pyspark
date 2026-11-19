"""Generate small synthetic pharma commercial datasets (no real data)."""
import csv
import os
import random
from datetime import date, timedelta

random.seed(7)
OUT = "data/raw"
BRANDS = ["Cardiomax", "Oncoviva"]
SPECIALTIES = ["CARD", "ONC", "PCP", "NEURO"]
ZIPS = ["10001", "10452", "07030", "94105", "94612", "60601", "60614", "02139"]
WEEKS = [date(2026, 1, 5) + timedelta(weeks=i) for i in range(26)]
HCPS = [f"H{i:04d}" for i in range(1, 41)]


def write(name, header, rows):
    os.makedirs(OUT, exist_ok=True)
    with open(f"{OUT}/{name}", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main():
    master = {h: (random.choice(SPECIALTIES), random.choice(ZIPS)) for h in HCPS}
    write("hcp_master_2026_01.csv", ["hcp_id", "specialty", "zip"],
          [(h, s, z) for h, (s, z) in master.items()])

    # Second snapshot: 3 HCPs change specialty, 2 new HCPs appear -> exercises SCD Type 2
    for h in HCPS[:3]:
        s, z = master[h]
        master[h] = (next(x for x in SPECIALTIES if x != s), z)
    master["H0041"], master["H0042"] = ("ONC", "10001"), ("PCP", "60601")
    write("hcp_master_2026_04.csv", ["hcp_id", "specialty", "zip"],
          [(h, s, z) for h, (s, z) in master.items()])

    rx = []
    for h in HCPS:
        zip_code = master[h][1]
        for b in random.sample(BRANDS, k=random.randint(1, 2)):
            for wk in WEEKS:
                trx = random.randint(0, 12)
                rx.append((h, zip_code, b, wk.isoformat(), trx, random.randint(0, trx)))
    rx.append(rx[0])                                        # duplicate row
    rx.append(("H0005", "10001", "Cardiomax", WEEKS[3].isoformat(), -4, 0))  # bad value
    write("rx_weekly.csv", ["hcp_id", "zip", "brand", "week_start", "trx", "nrx"], rx)

    calls = [(h, wk.isoformat(), random.randint(0, 3)) for h in HCPS for wk in WEEKS]
    write("rep_calls.csv", ["hcp_id", "week_start", "calls"], calls)
    print(f"Wrote synthetic files to {OUT}/")


if __name__ == "__main__":
    main()
