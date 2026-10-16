#!/usr/bin/env python3
"""Configuration-driven, robots-aware web data collector.

Users define what to collect in a JSON job file: variables, sources, crawl
boundaries, fields, CSS selectors, JSON/JSON-LD paths, validation filters,
deduplication keys, target size, and CSV/Markdown output.

Install HTTP/HTML dependencies:
    python3 -m pip install requests beautifulsoup4

Create a documented starter job:
    python3 generic_web_scraper.py --init-config scraper-job.json

Run it:
    python3 generic_web_scraper.py --config scraper-job.json --verbose

Or launch the local browser interface:
    python3 generic_web_scraper.py --web

The tool intentionally does not render JavaScript, solve CAPTCHAs, authenticate,
or bypass access controls. Use only sources you are permitted to collect and
keep the default robots.txt and rate-limit safeguards enabled.
"""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import logging
import os
import re
import secrets
import sys
import tempfile
import threading
import time
import warnings
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Sequence
from urllib.parse import quote_plus, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
import webbrowser

# Apple's system Python links against LibreSSL and an older system Tk. Their
# libraries emit startup-only compatibility/deprecation notices even though the
# features used by this local app continue to work. Set these before importing
# requests/urllib3 or Tkinter so the GUI opens without terminal warning noise.
os.environ.setdefault("TK_SILENCE_DEPRECATION", "1")
warnings.filterwarnings(
    "ignore",
    message=r"urllib3 v2 only supports OpenSSL 1\.1\.1\+.*",
    module=r"urllib3(?:\..*)?",
)

try:
    import requests
except ModuleNotFoundError:
    requests = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup, Tag
except ModuleNotFoundError:
    BeautifulSoup = None  # type: ignore[assignment,misc]
    Tag = Any  # type: ignore[misc,assignment]


LOG = logging.getLogger("generic-web-scraper")
REQUEST_EXCEPTION = requests.RequestException if requests is not None else OSError
DEFAULT_USER_AGENT = "ConfigurablePublicDataCollector/1.0"
SUPPORTED_TRANSFORMS = {
    "strip",
    "collapse_whitespace",
    "lower",
    "upper",
    "title",
    "digits",
    "phone",
    "email",
    "url",
    "absolute_url",
    "remove_html",
}


EXAMPLE_CONFIG: dict[str, Any] = {
    "_documentation": {
        "workflow": "Run --init-config, edit this JSON, then run --config. Use --set query=... to override variables.",
        "source_types": {
            "web": "Crawls permitted HTML pages using CSS selectors and/or schema.org JSON-LD.",
            "csv": "Imports existing CSV exports. column_map maps output field names to CSV headers.",
            "json_api": "Performs read-only GET requests. Set items_path and field json_paths; pagination may use next_url_path or next_token_path plus token_param.",
        },
        "field_options": "name, required, default, constant, selectors, json_paths, regex, regex_group, transforms, validate_regex, exclude_values, all, separator",
        "transforms": sorted(SUPPORTED_TRANSFORMS),
        "dedupe_keys": "Each inner list is an alternative unique key. A match on any complete key merges records.",
        "safety": "Keep obey_robots enabled and set narrow allowed_domains, follow_patterns, request limits, and delays.",
    },
    "name": "example-public-directory-job",
    "variables": {
        "query": "pet grooming",
        "location": "Singapore",
    },
    "target_count": 100,
    "allow_partial": False,
    "output": {
        "format": "csv",
        "path": "scraped-results.csv",
    },
    "crawler": {
        "user_agent": DEFAULT_USER_AGENT,
        "obey_robots": True,
        "delay_seconds": 1.0,
        "timeout_seconds": 20.0,
        "max_total_requests": 500,
        "max_response_bytes": 5000000,
    },
    "fields": [
        {
            "name": "Phone",
            "required": True,
            "selectors": [
                {"css": "a[href^='tel:']", "attribute": "href"},
                {"css": ".phone", "attribute": "text"},
            ],
            "json_paths": ["telephone", "phone"],
            "regex": "(?:\\+?\\d[\\d ().-]{6,}\\d)",
            "regex_group": 0,
            "transforms": ["phone"],
            "validate_regex": "^\\+?\\d{7,15}$",
        },
        {
            "name": "Contact Name",
            "required": False,
            "default": "N/A",
            "selectors": [".contact-name", ".owner", ".manager"],
            "json_paths": ["founder.name", "employee.0.name"],
            "transforms": ["collapse_whitespace"],
        },
        {
            "name": "Business Name",
            "required": True,
            "selectors": ["h1", ".business-name", ".listing-title"],
            "json_paths": ["name"],
            "transforms": ["collapse_whitespace"],
        },
        {
            "name": "Website",
            "required": False,
            "default": "N/A",
            "selectors": [
                {"css": "a.website", "attribute": "href"},
                {"css": "a[rel='external']", "attribute": "href"},
            ],
            "json_paths": ["sameAs.0", "website", "url"],
            "transforms": ["absolute_url", "url"],
        },
    ],
    "filters": {
        "must_match_any": ["(?i){query}"],
        "must_match_all": ["(?i){location}"],
        "must_not_match": [],
        "include_page_context": True,
    },
    "dedupe_keys": [
        ["Phone"],
        ["Business Name", "Website"],
    ],
    "sources": [
        {
            "type": "web",
            "name": "replace-with-a-permitted-directory",
            "start_urls": ["https://directory.example/search?q={query_url}&where={location_url}"],
            "allowed_domains": ["directory.example"],
            "follow_patterns": ["/listing/", "[?&]page=\\d+"],
            "exclude_patterns": ["/login", "/account"],
            "extract_url_patterns": ["/listing/"],
            "follow_selector": "main a[href]",
            "item_selector": "main",
            "modes": ["html", "jsonld"],
            "jsonld_types": [
                "HealthAndBeautyBusiness",
                "LocalBusiness",
                "Organization",
                "ProfessionalService",
                "Store",
            ],
            "max_pages": 150,
        },
        {
            "type": "csv",
            "name": "optional-existing-export",
            "paths": ["optional-seed-data.csv"],
            "optional": True,
            "column_map": {
                "Phone": "Phone",
                "Contact Name": "Contact Name",
                "Business Name": "Business Name",
                "Website": "Website",
            },
        },
    ],
}


class ScraperError(RuntimeError):
    """A configuration, collection, or target-size failure."""


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return html.unescape(str(value)).strip()


def normalize_url(value: Any, base_url: str = "") -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    if base_url:
        raw = urljoin(base_url, raw)
    if raw.startswith("//"):
        raw = "https:" + raw
    if not re.match(r"^https?://", raw, re.IGNORECASE):
        if re.match(r"^(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/|$)", raw):
            raw = "https://" + raw
        else:
            return ""
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""
    return urlunparse(
        parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            fragment="",
        )
    ).rstrip("/")


def is_placeholder_url(value: Any) -> bool:
    """Return True for documentation-only hostnames that can never be crawled."""
    parsed = urlparse(normalize_url(value))
    host = (parsed.hostname or "").casefold()
    return host in {"example.com", "example.net", "example.org"} or host.endswith(".example")


def transform_value(value: Any, transforms: Sequence[str], base_url: str) -> str:
    result = clean_text(value)
    for transform in transforms:
        if transform not in SUPPORTED_TRANSFORMS:
            raise ScraperError(f"unknown transform: {transform}")
        if transform == "strip":
            result = result.strip()
        elif transform == "collapse_whitespace":
            result = re.sub(r"\s+", " ", result).strip()
        elif transform == "lower":
            result = result.lower()
        elif transform == "upper":
            result = result.upper()
        elif transform == "title":
            result = result.title()
        elif transform == "digits":
            result = re.sub(r"\D", "", result)
        elif transform == "phone":
            has_plus = result.lstrip().startswith("+")
            digits = re.sub(r"\D", "", result)
            result = ("+" if has_plus and digits else "") + digits
        elif transform == "email":
            match = re.search(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", result, re.IGNORECASE)
            result = match.group(0).lower() if match else ""
        elif transform in {"url", "absolute_url"}:
            result = normalize_url(result, base_url if transform == "absolute_url" else "")
        elif transform == "remove_html":
            if BeautifulSoup is None:
                result = re.sub(r"<[^>]+>", " ", result)
                result = re.sub(r"\s+", " ", result).strip()
            else:
                result = BeautifulSoup(result, "html.parser").get_text(" ", strip=True)
    return result


def regex_extract(value: str, pattern: str, group: int | str | None) -> str:
    match = re.search(pattern, value)
    if not match:
        return ""
    if group is None:
        group = 1 if match.lastindex else 0
    try:
        return clean_text(match.group(group))
    except (IndexError, KeyError):
        raise ScraperError(f"regex group {group!r} does not exist in {pattern!r}")


def flatten_values(value: Any) -> Iterator[Any]:
    if isinstance(value, list):
        for item in value:
            yield from flatten_values(item)
    elif value is not None:
        yield value


def get_json_path(value: Any, path: str) -> Any:
    """Read a dotted path. Numeric segments index arrays; * maps remaining path."""

    if path in {"", "."}:
        return value
    segments = path.split(".")

    def walk(current: Any, index: int) -> Any:
        if index >= len(segments):
            return current
        segment = segments[index]
        if segment == "*":
            items = current.values() if isinstance(current, Mapping) else current if isinstance(current, list) else []
            return [walk(item, index + 1) for item in items]
        if isinstance(current, Mapping):
            return walk(current.get(segment), index + 1)
        if isinstance(current, list) and segment.isdigit():
            position = int(segment)
            return walk(current[position], index + 1) if position < len(current) else None
        return None

    return walk(value, 0)


def field_value(
    spec: Mapping[str, Any],
    raw_values: Iterable[Any],
    context: str,
    base_url: str,
) -> str:
    values = [clean_text(item) for raw in raw_values for item in flatten_values(raw) if clean_text(item)]
    pattern = clean_text(spec.get("regex"))
    if not values and pattern:
        values = [context]

    transformed: list[str] = []
    transforms = [str(item) for item in spec.get("transforms", [])]
    for raw in values:
        candidate = regex_extract(raw, pattern, spec.get("regex_group")) if pattern else raw
        if not candidate:
            continue
        candidate = transform_value(candidate, transforms, base_url)
        if not candidate:
            continue
        validation = clean_text(spec.get("validate_regex"))
        if validation and not re.search(validation, candidate):
            continue
        excluded = {clean_text(item).casefold() for item in spec.get("exclude_values", [])}
        if candidate.casefold() in excluded:
            continue
        transformed.append(candidate)
        if not spec.get("all"):
            break

    if transformed:
        separator = str(spec.get("separator", "; "))
        return separator.join(dict.fromkeys(transformed))
    return clean_text(spec.get("default"))


def selector_values(container: Tag, selector_specs: Sequence[Any]) -> Iterator[str]:
    for selector_spec in selector_specs:
        if isinstance(selector_spec, str):
            css = selector_spec
            attribute = "text"
            select_all = False
        elif isinstance(selector_spec, Mapping):
            css = clean_text(selector_spec.get("css"))
            attribute = clean_text(selector_spec.get("attribute")) or "text"
            select_all = bool(selector_spec.get("all"))
        else:
            raise ScraperError("field selectors must be strings or objects")
        if not css:
            continue
        try:
            matches = container.select(css)
        except Exception as exc:
            raise ScraperError(f"invalid CSS selector {css!r}: {exc}") from exc
        for element in matches:
            if attribute == "text":
                value = element.get_text(" ", strip=True)
            elif attribute == "html":
                value = element.decode_contents()
            else:
                value = element.get(attribute, "")
            if clean_text(value):
                yield clean_text(value)
                if not select_all:
                    break


def build_html_record(
    container: Tag,
    field_specs: Sequence[Mapping[str, Any]],
    page_url: str,
) -> dict[str, str]:
    context = container.get_text(" ", strip=True)
    record: dict[str, str] = {}
    for spec in field_specs:
        name = str(spec["name"])
        constant = spec.get("constant")
        values: Iterable[Any]
        if constant is not None:
            values = [constant]
        else:
            values = selector_values(container, spec.get("selectors", []))
        record[name] = field_value(spec, values, context, page_url)
    return record


def build_json_record(
    item: Any,
    field_specs: Sequence[Mapping[str, Any]],
    page_url: str,
) -> dict[str, str]:
    context = clean_text(item)
    record: dict[str, str] = {}
    for spec in field_specs:
        name = str(spec["name"])
        if spec.get("constant") is not None:
            values = [spec["constant"]]
        else:
            values = [get_json_path(item, str(path)) for path in spec.get("json_paths", [])]
        record[name] = field_value(spec, values, context, page_url)
    return record


def finalize_raw_record(
    raw: Mapping[str, Any],
    field_specs: Sequence[Mapping[str, Any]],
    base_url: str = "",
) -> dict[str, str]:
    context = " ".join(clean_text(value) for value in raw.values())
    record: dict[str, str] = {}
    for spec in field_specs:
        name = str(spec["name"])
        value = spec.get("constant") if spec.get("constant") is not None else raw.get(name)
        record[name] = field_value(spec, [value], context, base_url)
    return record


def validate_record(record: Mapping[str, str], fields: Sequence[Mapping[str, Any]]) -> bool:
    return all(not spec.get("required") or clean_text(record.get(str(spec["name"]))) for spec in fields)


def record_passes_filters(record: Mapping[str, str], context: str, filters: Mapping[str, Any]) -> bool:
    record_text = " ".join(record.values())
    haystack = record_text + (" " + context if filters.get("include_page_context") else "")
    try:
        must_all = [str(item) for item in filters.get("must_match_all", [])]
        must_any = [str(item) for item in filters.get("must_match_any", [])]
        must_not = [str(item) for item in filters.get("must_not_match", [])]
        return (
            all(re.search(pattern, haystack) for pattern in must_all)
            and (not must_any or any(re.search(pattern, haystack) for pattern in must_any))
            and not any(re.search(pattern, haystack) for pattern in must_not)
        )
    except re.error as exc:
        raise ScraperError(f"invalid record filter regex: {exc}") from exc


def normalized_key(value: str) -> str:
    if re.fullmatch(r"\+?\d{7,15}", value):
        return re.sub(r"\D", "", value)
    url = normalize_url(value)
    if url:
        return url.casefold()
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


class RecordIndex:
    def __init__(
        self,
        field_specs: Sequence[Mapping[str, Any]],
        dedupe_groups: Sequence[Sequence[str]],
    ) -> None:
        self.fields = [str(spec["name"]) for spec in field_specs]
        self.defaults = {str(spec["name"]): clean_text(spec.get("default")) for spec in field_specs}
        self.groups = [tuple(group) for group in dedupe_groups] or [tuple(self.fields)]
        self.records: list[dict[str, str]] = []
        self._keys: list[dict[tuple[str, ...], int]] = [dict() for _ in self.groups]

    def __len__(self) -> int:
        return len(self.records)

    def add(self, record: Mapping[str, str]) -> bool:
        clean = {field: clean_text(record.get(field)) or self.defaults[field] for field in self.fields}
        keys = [self._make_key(clean, group) for group in self.groups]
        existing = next(
            (
                index
                for key_map, key in zip(self._keys, keys)
                if key is not None and (index := key_map.get(key)) is not None
            ),
            None,
        )
        if existing is not None:
            merged = self.records[existing]
            for field in self.fields:
                if not merged[field] or merged[field] == self.defaults[field]:
                    merged[field] = clean[field]
            self._index(existing, merged)
            return False
        position = len(self.records)
        self.records.append(clean)
        self._index(position, clean)
        return True

    @staticmethod
    def _make_key(record: Mapping[str, str], fields: Sequence[str]) -> tuple[str, ...] | None:
        values = tuple(normalized_key(clean_text(record.get(field))) for field in fields)
        return values if all(values) else None

    def _index(self, position: int, record: Mapping[str, str]) -> None:
        for key_map, group in zip(self._keys, self.groups):
            key = self._make_key(record, group)
            if key is not None:
                key_map[key] = position


class HttpClient:
    def __init__(self, config: Mapping[str, Any]) -> None:
        if requests is None:
            raise ScraperError("missing dependency: python3 -m pip install requests beautifulsoup4")
        self.session = requests.Session()
        self.user_agent = clean_text(config.get("user_agent")) or DEFAULT_USER_AGENT
        self.session.headers.update({"User-Agent": self.user_agent, "Accept-Language": "en;q=0.9"})
        self.timeout = float(config.get("timeout_seconds", 20.0))
        self.delay = max(0.0, float(config.get("delay_seconds", 1.0)))
        self.obey_robots = bool(config.get("obey_robots", True))
        self.max_requests = max(1, int(config.get("max_total_requests", 500)))
        self.max_bytes = max(1024, int(config.get("max_response_bytes", 5000000)))
        self.request_count = 0
        self._last_request: dict[str, float] = {}
        self._robots: dict[str, RobotFileParser | None] = {}
        self._lock = threading.Lock()

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        obey_robots: bool | None = None,
    ) -> Any:
        current = normalize_url(url)
        if not current:
            raise ScraperError(f"invalid URL: {url!r}")
        redirects = 0
        while True:
            should_obey = self.obey_robots if obey_robots is None else obey_robots
            if should_obey and not self.allowed(current):
                raise ScraperError(f"robots.txt disallows: {current}")
            if self.request_count >= self.max_requests:
                raise ScraperError(f"global HTTP request cap reached ({self.max_requests})")
            self._pace(current)
            response = self.session.get(
                current,
                params=params if redirects == 0 else None,
                headers=dict(headers or {}),
                timeout=self.timeout,
                allow_redirects=False,
                stream=True,
            )
            self.request_count += 1
            if response.is_redirect or response.is_permanent_redirect:
                redirects += 1
                if redirects > 5:
                    response.close()
                    raise ScraperError(f"too many redirects: {url}")
                location = response.headers.get("Location", "")
                response.close()
                current = normalize_url(urljoin(current, location))
                if not current:
                    raise ScraperError(f"invalid redirect from {url}")
                continue
            response.raise_for_status()
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(chunk_size=65536):
                size += len(chunk)
                if size > self.max_bytes:
                    response.close()
                    raise ScraperError(f"response exceeds max_response_bytes: {current}")
                chunks.append(chunk)
            response._content = b"".join(chunks)
            response._content_consumed = True
            return response

    def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin not in self._robots:
            robots_url = origin + "/robots.txt"
            try:
                self._pace(robots_url)
                response = self.session.get(robots_url, timeout=self.timeout, allow_redirects=True)
                self.request_count += 1
                if response.status_code == 404:
                    self._robots[origin] = None
                else:
                    response.raise_for_status()
                    parser = RobotFileParser()
                    parser.set_url(robots_url)
                    parser.parse(response.text.splitlines())
                    self._robots[origin] = parser
            except REQUEST_EXCEPTION as exc:
                LOG.warning("Cannot verify %s (%s); blocking this host", robots_url, exc)
                parser = RobotFileParser()
                parser.parse(["User-agent: *", "Disallow: /"])
                self._robots[origin] = parser
        parser = self._robots[origin]
        return parser is None or parser.can_fetch(self.user_agent, url)

    def _pace(self, url: str) -> None:
        host = urlparse(url).netloc.casefold()
        with self._lock:
            elapsed = time.monotonic() - self._last_request.get(host, 0.0)
            wait = self.delay - elapsed
            if wait > 0:
                time.sleep(wait)
            self._last_request[host] = time.monotonic()


def json_objects(value: Any) -> Iterator[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        for child in value.values():
            yield from json_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from json_objects(child)


def object_types(item: Mapping[str, Any]) -> set[str]:
    value = item.get("@type")
    values = value if isinstance(value, list) else [value]
    return {clean_text(entry).casefold() for entry in values if clean_text(entry)}


def compile_patterns(values: Iterable[Any], label: str) -> list[re.Pattern[str]]:
    try:
        return [re.compile(str(value)) for value in values]
    except re.error as exc:
        raise ScraperError(f"invalid {label} regex: {exc}") from exc


class WebSource:
    def __init__(
        self,
        client: HttpClient,
        config: Mapping[str, Any],
        field_specs: Sequence[Mapping[str, Any]],
        filters: Mapping[str, Any],
    ) -> None:
        if BeautifulSoup is None:
            raise ScraperError("web sources require: python3 -m pip install beautifulsoup4")
        self.client = client
        self.config = config
        self.fields = field_specs
        self.filters = filters
        self.starts = [normalize_url(url) for url in config.get("start_urls", [])]
        if not self.starts or any(not url for url in self.starts):
            raise ScraperError("each web source needs valid start_urls")
        placeholders = [url for url in self.starts if is_placeholder_url(url)]
        if placeholders:
            raise ScraperError(
                "start_urls still contains the placeholder URL "
                f"{placeholders[0]!r}; open Query Settings and enter a real public directory URL"
            )
        configured_domains = [clean_text(item).casefold().removeprefix("www.") for item in config.get("allowed_domains", [])]
        self.allowed_domains = set(configured_domains) or {
            urlparse(url).netloc.casefold().removeprefix("www.") for url in self.starts
        }
        self.follow_patterns = compile_patterns(config.get("follow_patterns", []), "follow_patterns")
        self.exclude_patterns = compile_patterns(config.get("exclude_patterns", []), "exclude_patterns")
        self.extract_patterns = compile_patterns(config.get("extract_url_patterns", []), "extract_url_patterns")
        self.max_pages = max(1, int(config.get("max_pages", 100)))
        self.modes = {str(item).casefold() for item in config.get("modes", ["html"])}
        unknown_modes = self.modes - {"html", "jsonld"}
        if unknown_modes:
            raise ScraperError(f"unknown web extraction modes: {sorted(unknown_modes)}")

    def collect(self) -> Iterator[dict[str, str]]:
        queue = deque(self.starts)
        visited: set[str] = set()
        while queue and len(visited) < self.max_pages:
            url = normalize_url(queue.popleft())
            if not url or url in visited or not self._host_allowed(url):
                continue
            visited.add(url)
            try:
                response = self.client.get(
                    url,
                    headers={str(key): str(value) for key, value in self.config.get("headers", {}).items()},
                    obey_robots=self.config.get("obey_robots"),
                )
            except (REQUEST_EXCEPTION, ScraperError) as exc:
                LOG.warning("Skipping %s (%s)", url, exc)
                continue
            if not self._host_allowed(response.url):
                LOG.warning("Skipping redirect outside allowed_domains: %s", response.url)
                continue
            content_type = response.headers.get("Content-Type", "text/html").casefold()
            if "html" not in content_type:
                LOG.warning("Skipping non-HTML response: %s", response.url)
                continue
            soup = BeautifulSoup(response.text, "html.parser")
            page_context = f"{response.url} {soup.get_text(' ', strip=True)}"
            if self._should_extract(response.url):
                yield from self._extract(soup, response.url, page_context)
            for next_url in self._links(soup, response.url):
                if next_url not in visited:
                    queue.append(next_url)
        LOG.info("Web source %s visited %d page(s)", self.config.get("name", self.starts[0]), len(visited))

    def _extract(self, soup: BeautifulSoup, page_url: str, context: str) -> Iterator[dict[str, str]]:
        if "jsonld" in self.modes:
            wanted_types = {clean_text(item).casefold() for item in self.config.get("jsonld_types", [])}
            for script in soup.select('script[type="application/ld+json"]'):
                try:
                    payload = json.loads(script.string or script.get_text())
                except (json.JSONDecodeError, TypeError):
                    continue
                for item in json_objects(payload):
                    if wanted_types and not object_types(item).intersection(wanted_types):
                        continue
                    record = build_json_record(item, self.fields, page_url)
                    if validate_record(record, self.fields) and record_passes_filters(record, context, self.filters):
                        yield record

        if "html" in self.modes:
            item_selector = clean_text(self.config.get("item_selector")) or "body"
            try:
                items = soup.select(item_selector)
            except Exception as exc:
                raise ScraperError(f"invalid item_selector {item_selector!r}: {exc}") from exc
            for item in items:
                record = build_html_record(item, self.fields, page_url)
                if validate_record(record, self.fields) and record_passes_filters(record, context, self.filters):
                    yield record

    def _links(self, soup: BeautifulSoup, page_url: str) -> Iterator[str]:
        if not self.follow_patterns:
            return
        selector = clean_text(self.config.get("follow_selector")) or "a[href]"
        try:
            links = soup.select(selector)
        except Exception as exc:
            raise ScraperError(f"invalid follow_selector {selector!r}: {exc}") from exc
        seen: set[str] = set()
        for link in links:
            href = normalize_url(urljoin(page_url, clean_text(link.get("href"))))
            if (
                href
                and href not in seen
                and self._host_allowed(href)
                and any(pattern.search(href) for pattern in self.follow_patterns)
                and not any(pattern.search(href) for pattern in self.exclude_patterns)
            ):
                seen.add(href)
                yield href

    def _should_extract(self, url: str) -> bool:
        return not self.extract_patterns or any(pattern.search(url) for pattern in self.extract_patterns)

    def _host_allowed(self, url: str) -> bool:
        host = urlparse(url).netloc.casefold().removeprefix("www.")
        return host in self.allowed_domains


class CsvSource:
    def __init__(
        self,
        config: Mapping[str, Any],
        config_dir: Path,
        field_specs: Sequence[Mapping[str, Any]],
        filters: Mapping[str, Any],
    ) -> None:
        self.config = config
        self.config_dir = config_dir
        self.fields = field_specs
        self.filters = filters

    def collect(self) -> Iterator[dict[str, str]]:
        paths = self.config.get("paths", [])
        if not paths:
            raise ScraperError("each CSV source needs paths")
        column_map = {str(key): str(value) for key, value in self.config.get("column_map", {}).items()}
        field_names = [str(spec["name"]) for spec in self.fields]
        for configured_path in paths:
            path = Path(str(configured_path))
            if not path.is_absolute():
                path = self.config_dir / path
            if not path.exists() and self.config.get("optional"):
                LOG.info("Optional CSV does not exist: %s", path)
                continue
            try:
                handle = path.open("r", encoding=str(self.config.get("encoding", "utf-8-sig")), newline="")
            except OSError as exc:
                raise ScraperError(f"cannot open CSV {path}: {exc}") from exc
            with handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    raise ScraperError(f"CSV has no header: {path}")
                for row in reader:
                    raw = {
                        field: row.get(column_map.get(field, field), "")
                        for field in field_names
                    }
                    record = finalize_raw_record(raw, self.fields)
                    context = " ".join(clean_text(value) for value in row.values())
                    if validate_record(record, self.fields) and record_passes_filters(record, context, self.filters):
                        yield record


class JsonApiSource:
    """Read-only GET adapter for a permitted JSON endpoint."""

    def __init__(
        self,
        client: HttpClient,
        config: Mapping[str, Any],
        field_specs: Sequence[Mapping[str, Any]],
        filters: Mapping[str, Any],
    ) -> None:
        self.client = client
        self.config = config
        self.fields = field_specs
        self.filters = filters

    def collect(self) -> Iterator[dict[str, str]]:
        urls = [normalize_url(item) for item in self.config.get("start_urls", [])]
        if not urls or any(not item for item in urls):
            raise ScraperError("each json_api source needs valid start_urls")
        headers = {str(key): str(value) for key, value in self.config.get("headers", {}).items()}
        for header, env_name in self.config.get("headers_from_env", {}).items():
            value = os.environ.get(str(env_name))
            if value is None:
                raise ScraperError(f"missing environment variable {env_name} for API header {header}")
            headers[str(header)] = value
        base_params = {str(key): value for key, value in self.config.get("params", {}).items()}
        items_path = clean_text(self.config.get("items_path"))
        max_pages = max(1, int(self.config.get("max_pages", 20)))
        next_url_path = clean_text(self.config.get("next_url_path"))
        next_token_path = clean_text(self.config.get("next_token_path"))
        token_param = clean_text(self.config.get("token_param")) or "page_token"
        queue = deque((url, dict(base_params)) for url in urls)
        visited: set[str] = set()
        pages = 0
        while queue and pages < max_pages:
            url, request_params = queue.popleft()
            signature = url + "?" + json.dumps(request_params, sort_keys=True, default=str)
            if signature in visited:
                continue
            visited.add(signature)
            response = self.client.get(
                url,
                params=request_params,
                headers=headers,
                obey_robots=self.config.get("obey_robots"),
            )
            pages += 1
            try:
                payload = response.json()
            except ValueError as exc:
                raise ScraperError(f"invalid JSON from {response.url}") from exc
            items = get_json_path(payload, items_path) if items_path else payload
            if isinstance(items, Mapping):
                items = [items]
            if not isinstance(items, list):
                raise ScraperError(f"items_path {items_path!r} did not resolve to a list/object")
            for item in items:
                record = build_json_record(item, self.fields, response.url)
                context = clean_text(item)
                if validate_record(record, self.fields) and record_passes_filters(record, context, self.filters):
                    yield record
            if next_url_path:
                next_url = normalize_url(get_json_path(payload, next_url_path), response.url)
                if next_url:
                    queue.append((next_url, dict(base_params)))
            elif next_token_path:
                token = get_json_path(payload, next_token_path)
                if token:
                    next_params = dict(request_params)
                    next_params[token_param] = token
                    queue.append((url, next_params))


def expand_templates(value: Any, variables: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in variables:
                raise ScraperError(f"undefined template variable: {name}")
            return variables[name]

        # Substitute only named placeholders. This deliberately leaves regex
        # quantifiers such as {6,} and literal JSON/CSS braces untouched.
        return re.sub(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", replace, value)
    if isinstance(value, list):
        return [expand_templates(item, variables) for item in value]
    if isinstance(value, Mapping):
        return {key: expand_templates(item, variables) for key, item in value.items()}
    return value


def build_variables(config: Mapping[str, Any], overrides: Sequence[str]) -> dict[str, str]:
    variables = {str(key): clean_text(value) for key, value in config.get("variables", {}).items()}
    for override in overrides:
        if "=" not in override:
            raise ScraperError(f"--set must be KEY=VALUE, got {override!r}")
        key, value = override.split("=", 1)
        if not key.strip():
            raise ScraperError("--set variable name cannot be empty")
        variables[key.strip()] = value
    expanded = dict(variables)
    for key, value in variables.items():
        expanded[f"{key}_url"] = quote_plus(value)
    return expanded


def validate_config(config: Mapping[str, Any]) -> None:
    fields = config.get("fields")
    sources = config.get("sources")
    if not isinstance(fields, list) or not fields:
        raise ScraperError("config requires a non-empty fields list")
    names: list[str] = []
    for spec in fields:
        if not isinstance(spec, Mapping) or not clean_text(spec.get("name")):
            raise ScraperError("every field requires a name")
        name = clean_text(spec["name"])
        if name in names:
            raise ScraperError(f"duplicate field name: {name}")
        names.append(name)
        unknown = set(str(item) for item in spec.get("transforms", [])) - SUPPORTED_TRANSFORMS
        if unknown:
            raise ScraperError(f"field {name!r} uses unknown transforms: {sorted(unknown)}")
        for regex_key in ("regex", "validate_regex"):
            if spec.get(regex_key):
                try:
                    re.compile(str(spec[regex_key]))
                except re.error as exc:
                    raise ScraperError(f"field {name!r} has invalid {regex_key}: {exc}") from exc
    if not isinstance(sources, list) or not sources:
        raise ScraperError("config requires a non-empty sources list")
    supported = {"web", "csv", "json_api"}
    for source in sources:
        if not isinstance(source, Mapping) or source.get("type") not in supported:
            raise ScraperError(f"source type must be one of {sorted(supported)}")
    target = int(config.get("target_count", 0))
    if target < 1:
        raise ScraperError("target_count must be at least 1")
    field_set = set(names)
    for group in config.get("dedupe_keys", []):
        if not isinstance(group, list) or not group:
            raise ScraperError("each dedupe_keys entry must be a non-empty field-name list")
        unknown = set(str(item) for item in group) - field_set
        if unknown:
            raise ScraperError(f"dedupe_keys references unknown fields: {sorted(unknown)}")


def load_config(path: Path, overrides: Sequence[str]) -> tuple[dict[str, Any], Path]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ScraperError(f"cannot load config {path}: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise ScraperError("top-level config must be a JSON object")
    variables = build_variables(raw, overrides)
    config = expand_templates(raw, variables)
    validate_config(config)
    return dict(config), path.resolve().parent


def collect(config: Mapping[str, Any], config_dir: Path) -> tuple[list[dict[str, str]], list[str]]:
    fields = config["fields"]
    field_names = [str(spec["name"]) for spec in fields]
    filters = config.get("filters", {})
    dedupe_groups = config.get("dedupe_keys", [])
    index = RecordIndex(fields, dedupe_groups)
    target = int(config["target_count"])
    needs_http = any(source["type"] in {"web", "json_api"} for source in config["sources"])
    client = HttpClient(config.get("crawler", {})) if needs_http else None

    for source_config in config["sources"]:
        if len(index) >= target:
            break
        source_type = source_config["type"]
        if source_type == "web":
            assert client is not None
            source = WebSource(client, source_config, fields, filters)
        elif source_type == "json_api":
            assert client is not None
            source = JsonApiSource(client, source_config, fields, filters)
        else:
            source = CsvSource(source_config, config_dir, fields, filters)
        for record in source.collect():
            index.add(record)
            if len(index) >= target:
                break
        LOG.info("After %s: %d unique record(s)", source_config.get("name", source_type), len(index))

    if len(index) < target and not config.get("allow_partial"):
        raise ScraperError(
            f"collected {len(index)} genuine unique record(s), fewer than target_count {target}; "
            "add sources or set allow_partial to true"
        )
    return index.records[:target], field_names


def render_csv(records: Sequence[Mapping[str, str]], fields: Sequence[str]) -> str:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(fields)
    writer.writerows([record.get(field, "") for field in fields] for record in records)
    return stream.getvalue()


def markdown_cell(value: Any) -> str:
    return clean_text(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def render_markdown(records: Sequence[Mapping[str, str]], fields: Sequence[str]) -> str:
    lines = [
        "| " + " | ".join(markdown_cell(field) for field in fields) + " |",
        "| " + " | ".join("---" for _ in fields) + " |",
    ]
    lines.extend(
        "| " + " | ".join(markdown_cell(record.get(field, "")) for field in fields) + " |"
        for record in records
    )
    return "\n".join(lines) + "\n"


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def run_self_test() -> None:
    assert transform_value(" +65 (9123) 4567 ", ["phone"], "") == "+6591234567"
    assert get_json_path({"items": [{"name": "A"}]}, "items.0.name") == "A"
    assert normalize_url("example.com/a#b") == "https://example.com/a"
    assert is_placeholder_url("https://directory.example/search")
    assert not is_placeholder_url("https://furvisor.com/cities/singapore/grooming")
    expanded_example = expand_templates(EXAMPLE_CONFIG, build_variables(EXAMPLE_CONFIG, []))
    assert expanded_example["fields"][0]["regex"] == EXAMPLE_CONFIG["fields"][0]["regex"]
    assert "pet+grooming" in expanded_example["sources"][0]["start_urls"][0]
    fields = [
        {"name": "Phone", "required": True, "transforms": ["phone"]},
        {"name": "Name", "required": True, "transforms": ["collapse_whitespace"]},
        {"name": "Site", "default": "N/A", "transforms": ["url"]},
    ]
    index = RecordIndex(fields, [["Phone"], ["Name", "Site"]])
    assert index.add(finalize_raw_record({"Phone": "+65 9123 4567", "Name": "  Alpha  Co "}, fields))
    assert not index.add(finalize_raw_record({"Phone": "+6591234567", "Name": "Alpha Co", "Site": "a.sg"}, fields))
    assert len(index) == 1 and index.records[0]["Site"] == "https://a.sg"
    rendered = render_csv(index.records, ["Phone", "Name", "Site"])
    assert rendered.splitlines()[0] == "Phone,Name,Site"
    if BeautifulSoup is not None:
        soup = BeautifulSoup('<main><h1>Example Ltd</h1><a href="tel:+6512345678">Call</a></main>', "html.parser")
        html_fields = [
            {"name": "Name", "required": True, "selectors": ["h1"], "transforms": ["collapse_whitespace"]},
            {
                "name": "Phone",
                "required": True,
                "selectors": [{"css": "a[href^='tel:']", "attribute": "href"}],
                "regex": r"\+?\d+",
                "regex_group": 0,
                "transforms": ["phone"],
            },
        ]
        record = build_html_record(soup.select_one("main"), html_fields, "https://example.com")
        assert record == {"Name": "Example Ltd", "Phone": "+6512345678"}

    gui_config = json.loads(json.dumps(EXAMPLE_CONFIG))
    apply_gui_values(
        gui_config,
        {
            "query": "cafes",
            "location": "Tokyo",
            "source_url": "https://example.com/search?q={query_url}",
            "item_selector": "article",
            "follow_pattern": "/page/\\d+",
            "target": "25",
            "max_pages": "50",
            "output_path": "cafes.csv",
            "output_format": "csv",
            "allow_partial": True,
        },
    )
    assert gui_config["variables"] == {"query": "cafes", "location": "Tokyo"}
    assert gui_config["target_count"] == 25
    assert gui_config["sources"][0]["item_selector"] == "article"


def first_web_source(config: Mapping[str, Any]) -> MutableMapping[str, Any] | None:
    for source in config.get("sources", []):
        if isinstance(source, MutableMapping) and source.get("type") == "web":
            return source
    return None


def gui_values_from_config(config: Mapping[str, Any]) -> dict[str, Any]:
    variables = config.get("variables", {})
    output = config.get("output", {})
    source = first_web_source(config) or {}
    starts = source.get("start_urls", [])
    follows = source.get("follow_patterns", [])
    return {
        "query": clean_text(variables.get("query")),
        "location": clean_text(variables.get("location")),
        "source_url": clean_text(starts[0]) if starts else "",
        "item_selector": clean_text(source.get("item_selector")) or "body",
        "follow_pattern": clean_text(follows[0]) if follows else "",
        "target": str(config.get("target_count", 100)),
        "max_pages": str(source.get("max_pages", 100)),
        "output_path": clean_text(output.get("path")) or "scraped-results.csv",
        "output_format": clean_text(output.get("format")) or "csv",
        "allow_partial": bool(config.get("allow_partial")),
    }


def apply_gui_values(config: MutableMapping[str, Any], values: Mapping[str, Any]) -> None:
    try:
        target = int(values.get("target", 0))
        max_pages = int(values.get("max_pages", 0))
    except (TypeError, ValueError) as exc:
        raise ScraperError("Target and maximum pages must be whole numbers") from exc
    if target < 1 or max_pages < 1:
        raise ScraperError("Target and maximum pages must be at least 1")

    variables = config.setdefault("variables", {})
    variables["query"] = clean_text(values.get("query"))
    variables["location"] = clean_text(values.get("location"))
    config["target_count"] = target
    config["allow_partial"] = bool(values.get("allow_partial"))

    output = config.setdefault("output", {})
    output["path"] = clean_text(values.get("output_path")) or "scraped-results.csv"
    output_format = clean_text(values.get("output_format")) or "csv"
    if output_format not in {"csv", "markdown"}:
        raise ScraperError("Output format must be csv or markdown")
    output["format"] = output_format

    source = first_web_source(config)
    if source is None:
        if not clean_text(values.get("source_url")):
            return
        source = {
            "type": "web",
            "name": "interface-web-source",
            "allowed_domains": [],
            "follow_patterns": [],
            "exclude_patterns": [],
            "extract_url_patterns": [],
            "modes": ["html", "jsonld"],
            "jsonld_types": [
                "HealthAndBeautyBusiness",
                "LocalBusiness",
                "Organization",
                "ProfessionalService",
                "Store",
            ],
        }
        config.setdefault("sources", []).insert(0, source)
    source_url = clean_text(values.get("source_url"))
    if source_url:
        source["start_urls"] = [source_url]
        parsed = urlparse(source_url.replace("{query_url}", "query").replace("{location_url}", "location"))
        if parsed.netloc:
            source["allowed_domains"] = [parsed.netloc.casefold().removeprefix("www.")]
    source["item_selector"] = clean_text(values.get("item_selector")) or "body"
    follow_pattern = clean_text(values.get("follow_pattern"))
    source["follow_patterns"] = [follow_pattern] if follow_pattern else []
    source["max_pages"] = max_pages


def launch_gui() -> int:
    try:
        import queue
        import tkinter as tk
        from tkinter import filedialog, messagebox, scrolledtext, ttk
    except ModuleNotFoundError as exc:
        raise ScraperError("Tkinter is not available in this Python installation") from exc

    class EventHandler(logging.Handler):
        def __init__(self, events: Any) -> None:
            super().__init__(logging.INFO)
            self.events = events
            self.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

        def emit(self, record: logging.LogRecord) -> None:
            self.events.put(("log", self.format(record)))

    class ScraperGui:
        def __init__(self, root: Any) -> None:
            self.root = root
            self.root.title("Generic Web Scraper")
            self.root.minsize(760, 680)
            self.events: Any = queue.Queue()
            self.raw_config: dict[str, Any] = json.loads(json.dumps(EXAMPLE_CONFIG))
            self.running = False

            script_dir = Path(__file__).resolve().parent
            self.config_path = tk.StringVar(value=str(script_dir / "scraper-job.json"))
            self.query = tk.StringVar()
            self.location = tk.StringVar()
            self.source_url = tk.StringVar()
            self.item_selector = tk.StringVar()
            self.follow_pattern = tk.StringVar()
            self.target = tk.StringVar()
            self.max_pages = tk.StringVar()
            self.output_path = tk.StringVar()
            self.output_format = tk.StringVar()
            self.allow_partial = tk.BooleanVar()
            self.status = tk.StringVar(value="Ready")

            outer = ttk.Frame(root, padding=14)
            outer.grid(row=0, column=0, sticky="nsew")
            root.rowconfigure(0, weight=1)
            root.columnconfigure(0, weight=1)
            outer.columnconfigure(1, weight=1)

            ttk.Label(outer, text="Generic Web Scraper", font=("TkDefaultFont", 16, "bold")).grid(
                row=0, column=0, columnspan=4, sticky="w", pady=(0, 12)
            )
            ttk.Label(outer, text="Job file").grid(row=1, column=0, sticky="w", pady=5)
            ttk.Entry(outer, textvariable=self.config_path).grid(row=1, column=1, sticky="ew", pady=5)
            ttk.Button(outer, text="Browse", command=self.browse_config).grid(row=1, column=2, padx=(6, 0), pady=5)
            ttk.Button(outer, text="New template", command=self.new_template).grid(
                row=1, column=3, padx=(6, 0), pady=5
            )
            self._entry_row(outer, 2, "Query", self.query)
            self._entry_row(outer, 3, "Location", self.location)
            self._entry_row(outer, 4, "Start URL", self.source_url)
            self._entry_row(outer, 5, "Item CSS selector", self.item_selector)
            self._entry_row(outer, 6, "Follow URL regex", self.follow_pattern)

            ttk.Label(outer, text="Target rows").grid(row=7, column=0, sticky="w", pady=5)
            ttk.Entry(outer, textvariable=self.target, width=12).grid(row=7, column=1, sticky="w", pady=5)
            ttk.Label(outer, text="Maximum pages").grid(row=7, column=2, sticky="e", padx=(12, 6), pady=5)
            ttk.Entry(outer, textvariable=self.max_pages, width=12).grid(row=7, column=3, sticky="w", pady=5)

            self._entry_row(outer, 8, "Output file", self.output_path, self.browse_output, "Browse")
            ttk.Label(outer, text="Output format").grid(row=9, column=0, sticky="w", pady=5)
            ttk.Combobox(
                outer,
                textvariable=self.output_format,
                values=("csv", "markdown"),
                state="readonly",
                width=14,
            ).grid(row=9, column=1, sticky="w", pady=5)
            ttk.Checkbutton(outer, text="Allow fewer rows than target", variable=self.allow_partial).grid(
                row=9, column=2, columnspan=2, sticky="w", padx=(12, 0), pady=5
            )

            buttons = ttk.Frame(outer)
            buttons.grid(row=10, column=0, columnspan=4, sticky="ew", pady=(12, 8))
            ttk.Button(buttons, text="Load", command=self.load_config).pack(side="left")
            ttk.Button(buttons, text="Save settings", command=self.save_config).pack(side="left", padx=6)
            ttk.Button(buttons, text="Advanced JSON", command=self.advanced_json).pack(side="left")
            self.run_button = ttk.Button(buttons, text="Run scraper", command=self.run_scraper)
            self.run_button.pack(side="right")

            ttk.Label(outer, textvariable=self.status).grid(row=11, column=0, columnspan=4, sticky="w", pady=(0, 5))
            self.log = scrolledtext.ScrolledText(outer, height=16, state="disabled", wrap="word")
            self.log.grid(row=12, column=0, columnspan=4, sticky="nsew")
            outer.rowconfigure(12, weight=1)

            LOG.setLevel(logging.INFO)
            self.log_handler = EventHandler(self.events)
            LOG.addHandler(self.log_handler)
            self.set_form(gui_values_from_config(self.raw_config))
            if Path(self.config_path.get()).exists():
                self.load_config()
            self.root.after(100, self.poll_events)

        @staticmethod
        def _entry_row(
            parent: Any,
            row: int,
            label: str,
            variable: Any,
            command: Any = None,
            button_text: str = "",
        ) -> None:
            ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=5)
            ttk.Entry(parent, textvariable=variable).grid(
                row=row, column=1, columnspan=2 if command else 3, sticky="ew", pady=5
            )
            if command:
                ttk.Button(parent, text=button_text, command=command).grid(row=row, column=3, padx=(6, 0), pady=5)

        def form_values(self) -> dict[str, Any]:
            return {
                "query": self.query.get(),
                "location": self.location.get(),
                "source_url": self.source_url.get(),
                "item_selector": self.item_selector.get(),
                "follow_pattern": self.follow_pattern.get(),
                "target": self.target.get(),
                "max_pages": self.max_pages.get(),
                "output_path": self.output_path.get(),
                "output_format": self.output_format.get(),
                "allow_partial": self.allow_partial.get(),
            }

        def set_form(self, values: Mapping[str, Any]) -> None:
            self.query.set(values["query"])
            self.location.set(values["location"])
            self.source_url.set(values["source_url"])
            self.item_selector.set(values["item_selector"])
            self.follow_pattern.set(values["follow_pattern"])
            self.target.set(values["target"])
            self.max_pages.set(values["max_pages"])
            self.output_path.set(values["output_path"])
            self.output_format.set(values["output_format"])
            self.allow_partial.set(values["allow_partial"])

        def browse_config(self) -> None:
            path = filedialog.askopenfilename(filetypes=(("JSON jobs", "*.json"), ("All files", "*")))
            if path:
                self.config_path.set(path)
                self.load_config()

        def new_template(self) -> None:
            path = filedialog.asksaveasfilename(
                defaultextension=".json",
                initialfile="scraper-job.json",
                filetypes=(("JSON jobs", "*.json"),),
            )
            if not path:
                return
            self.raw_config = json.loads(json.dumps(EXAMPLE_CONFIG))
            self.config_path.set(path)
            self.set_form(gui_values_from_config(self.raw_config))
            self.save_config()

        def browse_output(self) -> None:
            extension = ".md" if self.output_format.get() == "markdown" else ".csv"
            path = filedialog.asksaveasfilename(
                defaultextension=extension,
                filetypes=(("CSV", "*.csv"), ("Markdown", "*.md"), ("All files", "*")),
            )
            if path:
                self.output_path.set(path)

        def load_config(self) -> None:
            try:
                path = Path(self.config_path.get()).expanduser()
                with path.open("r", encoding="utf-8") as handle:
                    config = json.load(handle)
                if not isinstance(config, dict):
                    raise ScraperError("Top-level job config must be a JSON object")
                validate_config(config)
                self.raw_config = config
                self.set_form(gui_values_from_config(config))
                self.status.set(f"Loaded {path.name}")
                self.append_log(f"Loaded {path}")
            except (OSError, json.JSONDecodeError, ScraperError) as exc:
                messagebox.showerror("Cannot load job", str(exc))

        def save_config(self, show_message: bool = True) -> bool:
            try:
                path = Path(self.config_path.get()).expanduser()
                if not str(path):
                    raise ScraperError("Choose a job file first")
                apply_gui_values(self.raw_config, self.form_values())
                validate_config(self.raw_config)
                atomic_write(path, json.dumps(self.raw_config, indent=2, ensure_ascii=False) + "\n")
                self.status.set(f"Saved {path.name}")
                self.append_log(f"Saved {path}")
                if show_message:
                    messagebox.showinfo("Settings saved", str(path))
                return True
            except (OSError, ScraperError) as exc:
                messagebox.showerror("Cannot save job", str(exc))
                return False

        def advanced_json(self) -> None:
            editor = tk.Toplevel(self.root)
            editor.title("Advanced job JSON")
            editor.geometry("760x620")
            text = scrolledtext.ScrolledText(editor, wrap="none")
            text.pack(fill="both", expand=True, padx=10, pady=(10, 5))
            text.insert("1.0", json.dumps(self.raw_config, indent=2, ensure_ascii=False))

            def save_advanced() -> None:
                try:
                    config = json.loads(text.get("1.0", "end"))
                    if not isinstance(config, dict):
                        raise ScraperError("Top-level job config must be a JSON object")
                    validate_config(config)
                    self.raw_config = config
                    self.set_form(gui_values_from_config(config))
                    if self.save_config(show_message=False):
                        editor.destroy()
                except (json.JSONDecodeError, ScraperError) as exc:
                    messagebox.showerror("Invalid JSON", str(exc), parent=editor)

            controls = ttk.Frame(editor, padding=(10, 5, 10, 10))
            controls.pack(fill="x")
            ttk.Button(controls, text="Cancel", command=editor.destroy).pack(side="right")
            ttk.Button(controls, text="Save JSON", command=save_advanced).pack(side="right", padx=6)

        def run_scraper(self) -> None:
            if self.running or not self.save_config(show_message=False):
                return
            self.running = True
            self.run_button.configure(state="disabled")
            self.status.set("Running…")
            self.append_log("Starting scraper…")
            thread = threading.Thread(target=self._worker, daemon=True)
            thread.start()

        def _worker(self) -> None:
            try:
                path = Path(self.config_path.get()).expanduser()
                config, config_dir = load_config(path, [])
                records, fields = collect(config, config_dir)
                output = config.get("output", {})
                output_format = clean_text(output.get("format")) or "csv"
                content = render_csv(records, fields) if output_format == "csv" else render_markdown(records, fields)
                configured_output = output.get("path")
                if configured_output in {None, "", "-"}:
                    raise ScraperError("The desktop interface requires an output file path")
                output_path = Path(str(configured_output)).expanduser()
                if not output_path.is_absolute():
                    output_path = config_dir / output_path
                atomic_write(output_path, content)
                self.events.put(("done", f"Wrote {len(records)} record(s) to {output_path}"))
            except (ScraperError, OSError, REQUEST_EXCEPTION) as exc:
                self.events.put(("error", str(exc)))

        def poll_events(self) -> None:
            try:
                while True:
                    event, message = self.events.get_nowait()
                    if event == "log":
                        self.append_log(message)
                    elif event == "done":
                        self.running = False
                        self.run_button.configure(state="normal")
                        self.status.set("Complete")
                        self.append_log(message)
                        messagebox.showinfo("Scrape complete", message)
                    elif event == "error":
                        self.running = False
                        self.run_button.configure(state="normal")
                        self.status.set("Failed")
                        self.append_log(f"ERROR: {message}")
                        messagebox.showerror("Scrape failed", message)
            except queue.Empty:
                pass
            self.root.after(100, self.poll_events)

        def append_log(self, message: str) -> None:
            self.log.configure(state="normal")
            self.log.insert("end", message + "\n")
            self.log.see("end")
            self.log.configure(state="disabled")

    root = tk.Tk()
    try:
        ttk.Style(root).theme_use("aqua" if sys.platform == "darwin" else "clam")
    except tk.TclError:
        pass
    ScraperGui(root)
    root.mainloop()
    return 0


WEB_UI_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Generic Web Scraper</title>
  <style>
    :root{color-scheme:light;--bg:#f4f6f8;--panel:#fff;--ink:#17202a;--muted:#64748b;--line:#d9e0e7;--blue:#2563eb;--blue2:#1d4ed8;--green:#15803d;--red:#b91c1c;--shadow:0 12px 32px rgba(15,23,42,.08)}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
    .shell{max-width:1040px;margin:0 auto;padding:28px 20px 48px}.top{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:20px}
    h1{font-size:26px;line-height:1.2;margin:0}.subtitle{color:var(--muted);margin:5px 0 0}.status{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line);background:#fff;border-radius:999px;padding:8px 12px;font-weight:600;white-space:nowrap}
    .dot{width:9px;height:9px;border-radius:50%;background:#94a3b8}.status.running .dot{background:#f59e0b;animation:pulse 1s infinite}.status.complete .dot{background:#22c55e}.status.failed .dot{background:#ef4444}@keyframes pulse{50%{opacity:.35}}
    .panel{background:var(--panel);border:1px solid var(--line);border-radius:14px;box-shadow:var(--shadow);padding:20px;margin-bottom:16px}.panel h2{font-size:17px;margin:0 0 16px}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px 16px}.wide{grid-column:1/-1}.field label{display:block;font-weight:650;margin-bottom:6px}.hint{display:block;color:var(--muted);font-size:12px;margin-top:5px}
    input,select,textarea{width:100%;border:1px solid #cbd5e1;border-radius:8px;background:#fff;color:var(--ink);padding:10px 11px;font:inherit;outline:none}input:focus,select:focus,textarea:focus{border-color:#60a5fa;box-shadow:0 0 0 3px rgba(37,99,235,.12)}
    textarea{min-height:360px;resize:vertical;font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}.row{display:flex;gap:8px}.row input{flex:1}.check{display:flex;align-items:center;gap:8px;padding-top:28px}.check input{width:auto}
    button{border:0;border-radius:8px;padding:10px 14px;font:600 14px/1 inherit;cursor:pointer;background:#e8edf3;color:#17202a}button:hover{filter:brightness(.97)}button.primary{background:var(--blue);color:#fff}button.primary:hover{background:var(--blue2)}button.danger{background:#fee2e2;color:var(--red)}button:disabled{cursor:not-allowed;opacity:.55}
    .actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.actions .spacer{flex:1}.advanced summary{cursor:pointer;font-weight:650;list-style:none}.advanced summary::-webkit-details-marker{display:none}.advanced summary:after{content:"Show";float:right;color:var(--blue);font-weight:600}.advanced[open] summary:after{content:"Hide"}.advanced textarea{margin-top:14px}
    pre{margin:0;background:#101827;color:#dbeafe;border-radius:9px;padding:14px;min-height:180px;max-height:360px;overflow:auto;white-space:pre-wrap;word-break:break-word;font:12px/1.55 ui-monospace,SFMono-Regular,Menlo,monospace}
    .notice{display:none;border-radius:9px;padding:11px 13px;margin-bottom:14px}.notice.show{display:block}.notice.error{background:#fee2e2;color:#991b1b}.notice.ok{background:#dcfce7;color:#166534}
    @media(max-width:720px){.grid{grid-template-columns:1fr}.wide{grid-column:auto}.top{align-items:flex-start;flex-direction:column}.check{padding-top:0}.actions .spacer{display:none}}
  </style>
</head>
<body>
<main class="shell">
  <header class="top">
    <div><h1>Generic Web Scraper</h1><p class="subtitle">Configure a permitted source, save the job, and run it locally.</p></div>
    <div id="status" class="status"><span class="dot"></span><span id="statusText">Ready</span></div>
  </header>
  <div id="notice" class="notice"></div>

  <section class="panel">
    <h2>Job</h2>
    <div class="grid">
      <div class="field wide"><label for="jobPath">Job file</label><div class="row"><input id="jobPath" placeholder="/path/to/scraper-job.json"><button id="loadBtn">Load</button><button id="newBtn">New template</button></div></div>
      <div class="field"><label for="query">Query</label><input id="query" placeholder="pet grooming"></div>
      <div class="field"><label for="location">Location</label><input id="location" placeholder="Singapore"></div>
      <div class="field wide"><label for="sourceUrl">Start URL</label><input id="sourceUrl" placeholder="https://directory.example/search?q={query_url}&where={location_url}"><span class="hint">Use {query_url} and {location_url} to insert URL-encoded settings.</span></div>
      <div class="field"><label for="itemSelector">Item CSS selector</label><input id="itemSelector" placeholder="article.listing"></div>
      <div class="field"><label for="followPattern">Follow-link URL regex</label><input id="followPattern" placeholder="/listing/|[?&]page=\d+"></div>
      <div class="field"><label for="target">Target rows</label><input id="target" type="number" min="1"></div>
      <div class="field"><label for="maxPages">Maximum pages</label><input id="maxPages" type="number" min="1"></div>
      <div class="field wide"><label for="outputPath">Output file</label><input id="outputPath" placeholder="scraped-results.csv"></div>
      <div class="field"><label for="outputFormat">Output format</label><select id="outputFormat"><option value="csv">CSV</option><option value="markdown">Markdown</option></select></div>
      <label class="check"><input id="allowPartial" type="checkbox"> Allow fewer rows than target</label>
    </div>
  </section>

  <section class="panel advanced">
    <details id="advanced"><summary>Advanced JSON configuration</summary><textarea id="rawConfig" spellcheck="false"></textarea></details>
  </section>

  <section class="panel">
    <div class="actions">
      <button id="saveBtn">Save settings</button>
      <button id="refreshBtn">Refresh</button>
      <span class="spacer"></span>
      <button id="shutdownBtn" class="danger">Stop local server</button>
      <button id="runBtn" class="primary">Run scraper</button>
    </div>
  </section>

  <section class="panel"><h2>Run log</h2><pre id="log">Ready.</pre></section>
</main>
<script>
const TOKEN=__TOKEN_JSON__;
const ids=["jobPath","query","location","sourceUrl","itemSelector","followPattern","target","maxPages","outputPath","outputFormat","allowPartial","rawConfig","status","statusText","notice","log","runBtn"];
const el=Object.fromEntries(ids.map(id=>[id,document.getElementById(id)]));
let rawConfig={};
async function api(path,options={}){const headers={"X-Scraper-Token":TOKEN,...(options.headers||{})};if(options.body)headers["Content-Type"]="application/json";const response=await fetch(path,{...options,headers});const data=await response.json().catch(()=>({error:`HTTP ${response.status}`}));if(!response.ok)throw new Error(data.error||`HTTP ${response.status}`);return data}
function values(){return{query:el.query.value,location:el.location.value,source_url:el.sourceUrl.value,item_selector:el.itemSelector.value,follow_pattern:el.followPattern.value,target:el.target.value,max_pages:el.maxPages.value,output_path:el.outputPath.value,output_format:el.outputFormat.value,allow_partial:el.allowPartial.checked}}
function apply(data){if(data.job_path)el.jobPath.value=data.job_path;const v=data.values||{};el.query.value=v.query||"";el.location.value=v.location||"";el.sourceUrl.value=v.source_url||"";el.itemSelector.value=v.item_selector||"body";el.followPattern.value=v.follow_pattern||"";el.target.value=v.target||100;el.maxPages.value=v.max_pages||100;el.outputPath.value=v.output_path||"scraped-results.csv";el.outputFormat.value=v.output_format||"csv";el.allowPartial.checked=!!v.allow_partial;if(data.raw_config){rawConfig=data.raw_config;el.rawConfig.value=JSON.stringify(rawConfig,null,2)}updateStatus(data)}
function updateStatus(data){const status=(data.status||"ready").toLowerCase();el.status.className=`status ${status}`;el.statusText.textContent=status.charAt(0).toUpperCase()+status.slice(1);el.runBtn.disabled=status==="running";if(data.logs)el.log.textContent=data.logs.join("\n")||"Ready.";el.log.scrollTop=el.log.scrollHeight}
function notice(message,type="ok"){el.notice.textContent=message;el.notice.className=`notice show ${type}`;setTimeout(()=>{el.notice.className="notice"},5000)}
function payload(includeRaw=true){let parsed=rawConfig;if(includeRaw){try{parsed=JSON.parse(el.rawConfig.value)}catch(error){throw new Error(`Advanced JSON is invalid: ${error.message}`)}}return{job_path:el.jobPath.value,values:values(),raw_config:parsed}}
async function initial(){try{apply(await api("/api/state"))}catch(error){notice(error.message,"error")}}
document.getElementById("loadBtn").onclick=async()=>{try{apply(await api("/api/load",{method:"POST",body:JSON.stringify({job_path:el.jobPath.value})}));notice("Job loaded") }catch(error){notice(error.message,"error")}};
document.getElementById("newBtn").onclick=async()=>{try{apply(await api("/api/new",{method:"POST",body:JSON.stringify({job_path:el.jobPath.value})}));notice("New template created") }catch(error){notice(error.message,"error")}};
document.getElementById("saveBtn").onclick=async()=>{try{apply(await api("/api/save",{method:"POST",body:JSON.stringify(payload())}));notice("Settings saved") }catch(error){notice(error.message,"error")}};
document.getElementById("runBtn").onclick=async()=>{try{apply(await api("/api/run",{method:"POST",body:JSON.stringify(payload())}));notice("Scraper started") }catch(error){notice(error.message,"error")}};
document.getElementById("refreshBtn").onclick=initial;
document.getElementById("shutdownBtn").onclick=async()=>{if(!confirm("Stop the local scraper server?"))return;try{await api("/api/shutdown",{method:"POST",body:"{}"});document.body.innerHTML="<main class='shell'><section class='panel'><h2>Server stopped</h2><p>You can close this tab.</p></section></main>"}catch(error){notice(error.message,"error")}};
setInterval(async()=>{try{updateStatus(await api("/api/status"))}catch(_){}},1000);
initial();
</script>
</body>
</html>"""


def launch_web_ui(open_browser: bool = True) -> int:
    token = secrets.token_urlsafe(24)
    script_dir = Path(__file__).resolve().parent
    default_job = script_dir / "scraper-job.json"
    state_lock = threading.Lock()
    state: dict[str, Any] = {
        "job_path": str(default_job),
        "raw_config": json.loads(json.dumps(EXAMPLE_CONFIG)),
        "status": "ready",
        "logs": ["Ready."],
    }

    if default_job.exists():
        try:
            with default_job.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            if isinstance(loaded, dict):
                validate_config(loaded)
                state["raw_config"] = loaded
                state["logs"] = [f"Loaded {default_job}"]
        except (OSError, json.JSONDecodeError, ScraperError) as exc:
            state["logs"] = [f"Could not load {default_job}: {exc}"]

    def resolve_job_path(value: Any) -> Path:
        raw = clean_text(value)
        path = Path(raw).expanduser() if raw else default_job
        return path if path.is_absolute() else script_dir / path

    def snapshot() -> dict[str, Any]:
        with state_lock:
            raw = json.loads(json.dumps(state["raw_config"]))
            return {
                "job_path": state["job_path"],
                "raw_config": raw,
                "values": gui_values_from_config(raw),
                "status": state["status"],
                "logs": list(state["logs"][-500:]),
            }

    def append_log(message: str) -> None:
        with state_lock:
            state["logs"].append(message)
            state["logs"] = state["logs"][-500:]

    class WebLogHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__(logging.INFO)
            self.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

        def emit(self, record: logging.LogRecord) -> None:
            append_log(self.format(record))

    log_handler = WebLogHandler()
    LOG.setLevel(logging.INFO)
    LOG.addHandler(log_handler)

    def save_payload(payload: Mapping[str, Any], new_template: bool = False) -> dict[str, Any]:
        path = resolve_job_path(payload.get("job_path"))
        if new_template:
            config: dict[str, Any] = json.loads(json.dumps(EXAMPLE_CONFIG))
        else:
            supplied = payload.get("raw_config")
            with state_lock:
                fallback = json.loads(json.dumps(state["raw_config"]))
            config = supplied if isinstance(supplied, dict) else fallback
        values = payload.get("values")
        if isinstance(values, Mapping):
            apply_gui_values(config, values)
        validate_config(config)
        atomic_write(path, json.dumps(config, indent=2, ensure_ascii=False) + "\n")
        with state_lock:
            state["job_path"] = str(path)
            state["raw_config"] = config
        append_log(f"Saved {path}")
        return snapshot()

    def worker(job_path: Path) -> None:
        try:
            config, config_dir = load_config(job_path, [])
            records, fields = collect(config, config_dir)
            output = config.get("output", {})
            output_format = clean_text(output.get("format")) or "csv"
            content = render_csv(records, fields) if output_format == "csv" else render_markdown(records, fields)
            configured_output = output.get("path")
            if configured_output in {None, "", "-"}:
                raise ScraperError("The web interface requires an output file path")
            output_path = Path(str(configured_output)).expanduser()
            if not output_path.is_absolute():
                output_path = config_dir / output_path
            atomic_write(output_path, content)
            append_log(f"Completed: wrote {len(records)} record(s) to {output_path}")
            with state_lock:
                state["status"] = "complete"
        except (ScraperError, OSError, REQUEST_EXCEPTION) as exc:
            append_log(f"ERROR: {exc}")
            with state_lock:
                state["status"] = "failed"

    class Handler(BaseHTTPRequestHandler):
        server_version = "GenericWebScraper/1.0"

        def log_message(self, format_string: str, *args: Any) -> None:
            return

        def send_bytes(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, payload: Mapping[str, Any]) -> None:
            self.send_bytes(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

        def authorized(self) -> bool:
            return secrets.compare_digest(self.headers.get("X-Scraper-Token", ""), token)

        def read_json(self) -> Mapping[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ScraperError("Invalid Content-Length") from exc
            if length > 2_000_000:
                raise ScraperError("Request body is too large")
            try:
                payload = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError as exc:
                raise ScraperError(f"Invalid JSON request: {exc}") from exc
            if not isinstance(payload, Mapping):
                raise ScraperError("Request must contain a JSON object")
            return payload

        def do_GET(self) -> None:
            if self.path == f"/{token}":
                body = WEB_UI_HTML.replace("__TOKEN_JSON__", json.dumps(token)).encode("utf-8")
                self.send_bytes(200, body, "text/html; charset=utf-8")
                return
            if self.path in {"/api/state", "/api/status"}:
                if not self.authorized():
                    self.send_json(403, {"error": "Forbidden"})
                    return
                data = snapshot()
                if self.path == "/api/status":
                    data = {"status": data["status"], "logs": data["logs"]}
                self.send_json(200, data)
                return
            self.send_json(404, {"error": "Not found"})

        def do_POST(self) -> None:
            if not self.authorized():
                self.send_json(403, {"error": "Forbidden"})
                return
            try:
                payload = self.read_json()
                if self.path == "/api/load":
                    path = resolve_job_path(payload.get("job_path"))
                    with path.open("r", encoding="utf-8") as handle:
                        config = json.load(handle)
                    if not isinstance(config, dict):
                        raise ScraperError("Top-level job config must be a JSON object")
                    validate_config(config)
                    with state_lock:
                        state["job_path"] = str(path)
                        state["raw_config"] = config
                    append_log(f"Loaded {path}")
                    self.send_json(200, snapshot())
                    return
                if self.path == "/api/new":
                    self.send_json(200, save_payload(payload, new_template=True))
                    return
                if self.path == "/api/save":
                    self.send_json(200, save_payload(payload))
                    return
                if self.path == "/api/run":
                    with state_lock:
                        if state["status"] == "running":
                            raise ScraperError("A scrape is already running")
                    saved = save_payload(payload)
                    job_path = Path(saved["job_path"])
                    with state_lock:
                        state["status"] = "running"
                        state["logs"] = [f"Starting {job_path}…"]
                    threading.Thread(target=worker, args=(job_path,), daemon=True).start()
                    self.send_json(202, snapshot())
                    return
                if self.path == "/api/shutdown":
                    self.send_json(200, {"ok": True})
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                    return
                self.send_json(404, {"error": "Not found"})
            except (OSError, json.JSONDecodeError, ScraperError) as exc:
                self.send_json(400, {"error": str(exc)})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    url = f"http://127.0.0.1:{port}/{token}"
    print(f"Web interface: {url}")
    print("Press Ctrl+C in this terminal to stop it.")
    if open_browser:
        webbrowser.open(url, new=1)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        LOG.removeHandler(log_handler)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a configuration-driven public web data collection job.")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--config", type=Path, help="JSON job definition")
    action.add_argument("--init-config", type=Path, help="write a documented starter JSON job")
    action.add_argument("--self-test", action="store_true", help="run built-in offline checks")
    action.add_argument(
        "--web",
        "--gui",
        dest="web",
        action="store_true",
        help="launch the local browser settings interface (--gui is an alias)",
    )
    parser.add_argument("--no-browser", action="store_true", help="start --web without opening a browser tab")
    parser.add_argument("--set", action="append", default=[], metavar="KEY=VALUE", help="override a config variable")
    parser.add_argument("--output", type=Path, help="override output.path; use - for stdout")
    parser.add_argument("--format", choices=("csv", "markdown"), help="override output.format")
    parser.add_argument("--target", type=int, help="override target_count")
    parser.add_argument("--allow-partial", action="store_true", help="emit fewer records than target")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )
    try:
        if args.web:
            return launch_web_ui(open_browser=not args.no_browser)
        if args.self_test:
            run_self_test()
            print("Self-test passed.")
            return 0
        if args.init_config:
            atomic_write(args.init_config, json.dumps(EXAMPLE_CONFIG, indent=2, ensure_ascii=False) + "\n")
            print(str(args.init_config))
            return 0

        config, config_dir = load_config(args.config, args.set)
        if args.target is not None:
            if args.target < 1:
                raise ScraperError("--target must be at least 1")
            config["target_count"] = args.target
        if args.allow_partial:
            config["allow_partial"] = True
        output_config: MutableMapping[str, Any] = dict(config.get("output", {}))
        output_format = args.format or clean_text(output_config.get("format")) or "csv"
        if output_format not in {"csv", "markdown"}:
            raise ScraperError("output format must be csv or markdown")
        records, fields = collect(config, config_dir)
        content = render_csv(records, fields) if output_format == "csv" else render_markdown(records, fields)

        configured_output = args.output if args.output is not None else output_config.get("path")
        if configured_output in {None, "", "-"} or str(configured_output) == "-":
            sys.stdout.write(content)
        else:
            output_path = Path(str(configured_output))
            if not output_path.is_absolute():
                output_path = config_dir / output_path
            atomic_write(output_path, content)
            LOG.info("Wrote %d record(s) to %s", len(records), output_path)
        return 0
    except (ScraperError, OSError, REQUEST_EXCEPTION) as exc:
        LOG.error("%s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
