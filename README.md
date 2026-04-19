# Canadian Recession Watch

A simple, static dashboard tracking six macroeconomic indicators against the
twelve Canadian recessions identified by the [CD Howe Institute Business Cycle
Council](https://cdhowe.org/council/business-cycle-council/). The dashboard
updates daily through a GitHub Actions workflow that pulls fresh data from the
Bank of Canada's Valet API and Statistics Canada's Web Data Service.

## Live dashboard

Hosted on GitHub Pages at `https://<your-github-username>.github.io/canada-recession-watch/`.

## Indicators (v1 core)

| # | Indicator | Source | Series |
|---|---|---|---|
| 1 | Yield curve spread (10Y GoC − 3M T-bill) | StatsCan 10-10-0122 | v122543, v122541 |
| 2 | Unemployment rate, SA | StatsCan 14-10-0287 (LFS) | v2062815 |
| 3 | Housing starts, SAAR Canada | StatsCan 34-10-0158 (CMHC) | v52300157 |
| 4 | Retail trade sales YoY | StatsCan 20-10-0008 (legacy) + 20-10-0056 | v52367097 → v1446859483 |
| 5 | BoC Commodity Price Index YoY | BoC Valet | M.BCPI |
| 6 | CPI (headline + trim + median) | StatsCan 18-10-0004 + BoC Valet | v41690973, CPI_TRIM, CPI_MEDIAN |

Retail sales are spliced across a 2018-12 table transition. Where both series
overlap, their YoY growth rates agree within approximately 1 percentage point,
which is within noise for a recession-signal dashboard. The splice uses the
old series' YoY up to and including 2018-12, and the new series' YoY from
2019-01 onward.

## Signal rules

Each card is coloured by a simple rule. None of these rules is sufficient on
its own — they're a visual shorthand, not a substitute for judgement.

| Indicator | Green | Yellow | Red |
|---|---|---|---|
| Yield curve | spread ≥ 0.5pp | 0 to 0.5pp | inverted (< 0) |
| Unemployment | Sahm gap ≤ 0.3pp | 0.3 to 0.5pp | ≥ 0.5pp (Sahm rule triggered) |
| Housing starts | 3m vs 12m avg ≥ −5% | −5% to −10% | < −10% |
| Retail sales YoY | ≥ 1% | 0 to 1% | < 0% |
| BCPI YoY | ≥ −5% | −5% to −20% | < −20% |
| CPI-trim | 1–3% (in BoC band) | outside band | — |

The Sahm rule computes the 3-month average unemployment rate and compares it
to the minimum of the 12 months preceding that window; a gap of 0.5pp or more
historically triggers early in US recessions and has a respectable track
record in Canada.

## Architecture

- **`scripts/build_dataset.py`** — idempotent data pipeline. Fetches each
  indicator, normalises to monthly frequency, computes YoY transformations,
  handles the retail-sales splice, and writes `data/data.json`. Series-level
  failures are logged but do not clobber the prior `data.json`.
- **`data/data.json`** — the snapshot. Overwritten on every successful build.
  Roughly 250 KB. Each series carries `last_observation`, `last_value`,
  `source`, `source_url`, and `recession_signal` metadata alongside the
  monthly data array.
- **`data/cdhowe_recessions.json`** — the CD Howe Business Cycle Council
  vintage used for recession shading. Static; update manually if the Council
  publishes a new vintage.
- **`index.html`** — the dashboard. One file, no build step. Loads
  `data/data.json` and `data/cdhowe_recessions.json`, renders six Chart.js
  charts with recession shading, and computes signal colours client-side.
- **`.github/workflows/refresh.yml`** — daily cron at 12:30 UTC that runs the
  pipeline and commits `data.json` if it changed.

## Local development

### Refresh the data locally

```bash
pip install -r requirements.txt
python scripts/build_dataset.py
```

This will overwrite `data/data.json` with a fresh snapshot.

### Preview the dashboard

The dashboard fetches JSON, so opening `index.html` directly via `file://`
won't work (CORS blocks fetch from the local filesystem). Use a small HTTP
server:

```bash
python3 -m http.server 8765
```

Then open `http://localhost:8765/` in your browser.

## Deploying to GitHub Pages

1. Push this repo to GitHub (public repo if you want free Pages hosting).
2. In **Settings → Pages**, set source to `Deploy from a branch` and select
   `main / (root)`.
3. In **Settings → Actions → General → Workflow permissions**, set
   `Read and write permissions` so the workflow can commit the refreshed
   `data.json`.
4. The workflow will run automatically at 12:30 UTC daily, or you can trigger
   it manually from the **Actions** tab.

## Known limitations

- **Nominal retail sales.** The retail-sales YoY is nominal, not deflated,
  and includes autos and gasoline. In the 2021–22 inflation surge this flatters
  the growth rate significantly. A v2 improvement would deflate by CPI and
  strip the NAICS sub-sectors for autos and energy.
- **CPI-trim starts in 1995.** The BoC's modern core measures aren't
  available earlier. The dashboard still renders the chart but earlier
  recessions fall outside the axis.
- **Signal rules are crude.** They're single-variable, point-in-time, and
  don't account for direction of change, momentum, or interaction between
  indicators. The composite recession probability models in the academic
  literature (Estrella–Mishkin, Sahm, etc.) are more sophisticated. The
  dashboard is meant for at-a-glance orientation, not a forecast.
- **Schema drift.** StatsCan occasionally renames vectors. If the build
  starts failing for one series, check that the vector still points to what
  we think it does — the probe sequence in `scripts/build_dataset.py` shows
  how each was originally located.

## Methodology notes and caveats

The CD Howe Council's recession dates differ modestly from the NBER-analogue
dating you might see in other Canadian dashboards. The Council uses a peak
month as the last month of expansion (not included in the recession) and a
trough month as the last month of the recession (included). The 2020 COVID
episode is dated as a two-month recession (March–April 2020), reflecting its
unusual depth and brevity. The pre-1990 recessions are included in the
reference JSON but fall outside the 1990+ history window that this dashboard
currently displays.

## License

Data is redistributed under the terms of the respective source licenses
(Statistics Canada Open Licence, Bank of Canada terms of use). The code in
this repository is released under the MIT License — see `LICENSE` if added.
