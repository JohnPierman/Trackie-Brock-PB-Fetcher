# Trackie PB Fetcher

Scrapes [Trackie](https://www.trackie.com/) U SPORTS athlete pages to compute **personal bests (PBs)** for any Canadian university track & field team and exports them to CSV.

Includes both a **command-line interface** and a **graphical desktop application**.

## Features

- Scrapes athlete profile pages and historical rankings from Trackie
- Computes personal bests per event across a configurable number of seasons
- Correctly classifies track events (lower is better) and field events (higher is better)
- Discovers past athletes from historical rankings pages
- Concurrent scraping with polite rate-limiting
- Filters by sex, event, and free-text search
- Sortable results table with CSV export
- Works with any U SPORTS university, not just Brock

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## GUI

Launch the graphical interface:

```bash
python3 gui.py
```

The GUI provides:
- Configurable university URL, seasons back, and concurrency
- Real-time progress tracking during scraping
- Searchable, sortable, filterable results table
- One-click CSV export
- Load previously exported CSV files
- Double-click any row to open the athlete's Trackie profile
- Keyboard shortcuts (Cmd/Ctrl+R to scrape, Cmd/Ctrl+E to export, Cmd/Ctrl+L to load)

## Command Line

Default: last 5 seasons, Brock University.

```bash
python3 scrape_trackie_pbs.py --years-back 5 --out brock_pbs.csv
```

Current roster only (skip historical rankings discovery):

```bash
python3 scrape_trackie_pbs.py --years-back 5 --no-include-past-athletes --out brock_pbs.csv
```

All options:

```bash
python3 scrape_trackie_pbs.py \
  --university-url "https://www.trackie.com/usports/tnf/universities/brock-university/3/" \
  --years-back 5 \
  --delay-seconds 0.6 \
  --max-workers 6 \
  --out brock_pbs.csv
```

## Output

CSV rows are **one row per athlete per event PB**, including:

| Column | Description |
|---|---|
| `university` | University name (inferred from URL) |
| `athlete_name` | Athlete's full name |
| `sex` | M or F |
| `event` | Normalized event name |
| `pb_raw` | Raw performance string from Trackie |
| `pb_value` | Normalized numeric value |
| `pb_unit` | `s` (seconds), `m` (meters), or `pts` (points) |
| `better_is` | `lower` for track events, `higher` for field/combined |
| `pb_date` | ISO date (best-effort, inferred from season) |
| `pb_season` | Season label (e.g. `2025/26`) |
| `pb_meet` | Meet name |
| `pb_meet_url` | Link to meet results on Trackie |
| `athlete_url` | Link to athlete profile on Trackie |

## Notes & Limitations

- **Seasons, not calendar years** — "last N years" means last N Trackie seasons (Aug–Jul cycles).
- **Date inference** — Trackie often omits the year from dates. The scraper infers it from the season: Aug–Dec maps to the first year, Jan–Jul to the second.
- **Historical athletes** — Some athletes appear in rankings but have empty profile pages. For those, the scraper uses rankings-derived season-best marks (meet/date fields will be blank).
- **Rate limiting** — The scraper defaults to 0.6 s between requests to avoid overloading Trackie.

## License

MIT
