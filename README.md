# Generic Web Scraper

A configurable Python scraper with a local browser interface. It collects public directory data into CSV or Markdown without requiring you to edit Python code.

The included `scraper-job.json` is configured for public Singapore pet-grooming listings on Furvisor. You can replace that source and change the extraction rules for other public directories.

## Requirements

- Python 3.9 or newer
- An internet connection
- Permission to collect data from the chosen website

Install the two required packages:

### Windows

```powershell
py -m pip install requests beautifulsoup4
```

### macOS or Linux

```bash
python3 -m pip install requests beautifulsoup4
```

## Launch the browser interface

Open a terminal in this folder and run:

### Windows

```powershell
py generic_web_scraper.py --web
```

### macOS or Linux

```bash
python3 generic_web_scraper.py --web
```

The program starts a private local web server and opens the settings page in your default browser. Keep the terminal window open while using the interface.

The interface is available only on your computer through `127.0.0.1`; it is not published to the internet. If Windows Firewall asks for access, private/local network access is sufficient.

## Run a scrape

1. Review the query, location, source URL, target count, maximum pages, and output filename.
2. Click **Save settings**.
3. Click **Run scraper**.
4. Follow progress in the log panel.
5. Wait for the `Completed` message before opening the result file.

The default output is:

```text
scraped-results.csv
```

Relative output paths are saved beside `scraper-job.json`. You can enter an absolute output path if you want the result saved elsewhere.

## Important: query versus source URL

The query and location do not automatically find a directory. Every job needs at least one real source URL that contains the records you want to collect.

For example:

```text
https://furvisor.com/cities/singapore/grooming
```

Addresses such as `directory.example` are documentation placeholders and cannot be scraped. The program now stops with a clear error if a placeholder is used.

## Changing what is collected

The simple settings control:

- Query and location variables
- Starting source URL
- Listing/item CSS selector
- Link-following pattern
- Target record count
- Maximum pages
- CSV or Markdown output
- Whether partial results are allowed

Use the JSON editor in the interface for full control over fields, selectors, JSON-LD paths, filters, deduplication keys, crawl boundaries, and additional sources.

Each field can use:

- CSS selectors from HTML
- Paths from JSON or schema.org JSON-LD
- Regular expressions
- Cleanup transforms such as `phone`, `email`, and `url`
- Required-field and validation rules

The scraper can read three source types:

- `web` — public HTML pages and JSON-LD
- `csv` — existing CSV exports
- `json_api` — read-only public JSON endpoints

## Command-line use

Run the saved job without opening the interface:

### Windows

```powershell
py generic_web_scraper.py --config scraper-job.json --verbose
```

### macOS or Linux

```bash
python3 generic_web_scraper.py --config scraper-job.json --verbose
```

Useful options:

```text
--target 50                 Override the target count
--allow-partial             Save fewer records if the target is not reached
--output results.csv        Override the output filename
--format csv                Select CSV output
--format markdown           Select Markdown output
--set query="dog grooming" Override a job variable
--no-browser                Start the web interface without opening a tab
--self-test                 Run offline checks
```

## How collection works

The scraper:

1. Loads and validates `scraper-job.json`.
2. Checks `robots.txt` before crawling web pages.
3. Applies the configured request delay and crawl limits.
4. Follows only permitted domains and matching URLs.
5. Extracts and validates the configured fields.
6. Deduplicates records using the configured keys.
7. Writes the result atomically when collection finishes.

It does not render JavaScript, log into websites, solve CAPTCHAs, or bypass access controls. A heavily JavaScript-driven directory may require an official API or exported data instead.

## Troubleshooting

### The browser does not open

Copy the local URL printed in the terminal and paste it into your browser. You can also launch with `--no-browser` and open the printed URL manually.

### The result contains zero records

Check that:

- The source URL opens normally in a browser.
- The page is not entirely JavaScript-rendered.
- The item selector and field selectors match the page.
- The extraction URL pattern includes the individual listing pages.
- Required fields exist on those pages.
- Filters are not excluding every record.
- `robots.txt` permits the pages.

### Fewer records are available than the target

Enable **Allow partial results**, lower the target, or add another permitted source. The scraper never fabricates records to reach a target.

### A website changes its layout

Update the CSS selectors or JSON paths in the job's JSON editor. Directory layouts and structured-data fields can change over time.

## Files

```text
generic_web_scraper.py  Application and scraping engine
scraper-job.json        Saved query, source, and extraction configuration
scraped-results.csv     Default generated output
README.md               This guide
```

## Responsible use

Use only public sources you are permitted to collect. Follow applicable terms, privacy rules, and data-protection laws. Keep `obey_robots` enabled and use reasonable delays and request limits.
