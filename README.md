## Trackie Brock PB Fetcher

Scrapes Trackie U SPORTS pages to compute **personal bests (PBs)** for Brock University athletes over the **last N seasons** and exports them to a CSV.

- University page: `https://www.trackie.com/usports/tnf/universities/brock-university/3/`

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run

Default: last 5 seasons, Brock University.

```bash
python3 scrape_trackie_pbs.py --years-back 5 --out brock_pbs.csv
```

By default it also includes **past athletes** by discovering Brock athletes from the **historical Trackie rankings pages** within the same `--years-back` window. If you only want the current roster list:

```bash
python3 scrape_trackie_pbs.py --years-back 5 --no-include-past-athletes --out brock_pbs.csv
```

Common knobs:

```bash
python3 scrape_trackie_pbs.py \
  --years-back 5 \
  --delay-seconds 0.6 \
  --max-workers 6 \
  --out brock_pbs.csv
```

### Output

CSV rows are **one athlete per event PB**, including:

- athlete name + profile URL
- event name
- best performance (raw + normalized numeric)
- meet name + meet URL (when available)
- derived date (best-effort; Trackie often omits the year, so we infer it from season)

### Notes / Limitations

- “Last N years” is implemented as **last N seasons** (based on Trackie’s “Performance YYYY/YY” sections).
- Trackie sometimes displays dates without a year (e.g., “Nov 29th”). The script infers year from the season:
  - Aug–Dec → first year of season (e.g. 2025/26 → 2025)
  - Jan–Jul → second year of season (e.g. 2025/26 → 2026)
- Some historical athletes appear in Trackie’s rankings pages but have **empty/removed athlete profile pages**. For those athletes, the scraper falls back to **rankings-derived season-best marks**, which means:
  - `pb_meet`, `pb_meet_url`, `pb_date`, and `pb_date_raw` will be blank for those rows
  - PBs still respect `--years-back` because the rankings pages are fetched per season


