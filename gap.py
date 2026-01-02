from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests


GAP_PRODUCTS_CC_API = "https://api.gap.com/commerce/search/products/v2/cc"
GAP_PRODUCTS_STYLE_API = "https://api.gap.com/commerce/search/products/v2/style"
GAP_BASE = "https://www.gap.com"


@dataclass(frozen=True)
class PowerReviewsConfig:
	merchant_id: str
	api_key: str
	locale: str = "en_US"


@dataclass(frozen=True)
class ProductRow:
	brand_name: str
	full_name: str
	price: Optional[float]
	product_rating: Optional[float]
	customer_reviews_count: Optional[int]
	product_url: str
	customer_reviews_last_12_months_count: Optional[int]
	customer_reviews_2020_count: Optional[int]
	customer_reviews_2021_count: Optional[int]
	customer_reviews_2022_count: Optional[int]
	customer_reviews_2023_count: Optional[int]
	customer_reviews_2024_count: Optional[int]
	customer_reviews_2025_count: Optional[int]


def _requests_session() -> requests.Session:
	session = requests.Session()
	session.headers.update(
		{
			"User-Agent": (
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
				"AppleWebKit/537.36 (KHTML, like Gecko) "
				"Chrome/120.0.0.0 Safari/537.36"
			),
			"Accept": "application/json,text/plain,*/*",
		}
	)
	return session


def _safe_float(value: Any) -> Optional[float]:
	if value is None:
		return None
	try:
		return float(value)
	except Exception:
		return None


def _safe_int(value: Any) -> Optional[int]:
	if value is None:
		return None
	try:
		return int(value)
	except Exception:
		return None


def _min_price_from_style_colors(style_colors: List[Dict[str, Any]]) -> Optional[float]:
	prices: List[float] = []
	for color in style_colors or []:
		# Observed fields: effectivePrice, regularPrice
		eff = _safe_float(color.get("effectivePrice"))
		reg = _safe_float(color.get("regularPrice"))
		if eff is not None:
			prices.append(eff)
		elif reg is not None:
			prices.append(reg)
	return min(prices) if prices else None


def _first_ccid(style_colors: List[Dict[str, Any]]) -> Optional[str]:
	for color in style_colors or []:
		ccid = color.get("ccId") or color.get("id")
		if ccid:
			return str(ccid)
	return None


def _style_id_from_product(product: Dict[str, Any]) -> Optional[str]:
	value = product.get("styleId")
	if value is None:
		value = product.get("id")
	if value is None:
		return None
	return str(value)


def _style_name_from_product(product: Dict[str, Any]) -> str:
	name = product.get("styleName")
	if not name:
		name = product.get("name")
	return (str(name) if name is not None else "").strip()


def _colors_from_product(product: Dict[str, Any]) -> List[Dict[str, Any]]:
	colors = product.get("styleColors")
	if isinstance(colors, list):
		return colors
	colors = product.get("colors")
	if isinstance(colors, list):
		return colors
	return []


def _gap_products_api_url(*, cid: Optional[str], extra_params: Optional[Dict[str, str]]) -> str:
	# For keyword search pages, Gap uses the style endpoint and returns many more results.
	if extra_params and extra_params.get("keyword"):
		return GAP_PRODUCTS_STYLE_API
	return GAP_PRODUCTS_CC_API


def _price_from_style_color(style_color: Dict[str, Any]) -> Optional[float]:
	# Observed fields: effectivePrice, regularPrice
	eff = _safe_float(style_color.get("effectivePrice"))
	reg = _safe_float(style_color.get("regularPrice"))
	return eff if eff is not None else reg


def _parse_params_from_gap_url(url: str) -> Dict[str, str]:
	"""Extract query + fragment params from a Gap URL.

	Gap collection pages often store extra params (like department) in the fragment.
	We merge both into a flat string->string dict.
	"""
	parsed = urlparse(url)
	out: Dict[str, str] = {}
	for part in (parsed.query, parsed.fragment):
		if not part:
			continue
		for k, vs in parse_qs(part, keep_blank_values=True).items():
			if not vs:
				continue
			out[str(k)] = str(vs[-1])

	# Gap search pages use browse/search.do?searchText=... but the products API expects keyword=...
	if "searchText" in out and "keyword" not in out:
		out["keyword"] = out["searchText"]
		out.pop("searchText", None)
	return out


def fetch_gap_products(
	*,
	cid: Optional[str],
	locale: str,
	referer_url: str,
	session: requests.Session,
	page_sleep_s: float,
	extra_params: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
	products: List[Dict[str, Any]] = []
	page_number = 0
	total_pages: Optional[int] = None
	api_url = _gap_products_api_url(cid=cid, extra_params=extra_params)
	use_style_api = api_url == GAP_PRODUCTS_STYLE_API

	while True:
		params: Dict[str, str] = {
			"locale": locale,
			"pageNumber": str(page_number),
		}
		# The style-search endpoint doesn't accept cid; it uses keyword-based search.
		if cid and not use_style_api:
			params["cid"] = str(cid)
		# Ensure the API returns as many products per page as possible.
		if use_style_api and "pageSize" not in params:
			params["pageSize"] = "200"
		if extra_params:
			# Keep only values that the API is likely to accept; ignore anything empty.
			for k, v in extra_params.items():
				if k in {"cid", "locale", "pageNumber", "pageId"}:
					continue
				if v is None or str(v) == "":
					continue
				params[str(k)] = str(v)
		headers = {"Referer": referer_url}
		resp = session.get(api_url, params=params, headers=headers, timeout=30)
		resp.raise_for_status()
		data = resp.json()

		if total_pages is None:
			try:
				total_pages = int(data.get("pagination", {}).get("pageNumberTotal"))
			except Exception:
				total_pages = 1

		batch = data.get("products") or []
		if not isinstance(batch, list):
			raise RuntimeError("Unexpected products payload shape")
		products.extend(batch)

		page_number += 1
		if total_pages is not None and page_number >= total_pages:
			break

		if page_sleep_s:
			time.sleep(page_sleep_s)

	# Return raw API rows; caller decides whether to de-dupe (style vs color granularity).
	return products


_POWERREVIEWS_CONFIG_RE = re.compile(
	r"\\?\"powerReviewsConfig\\?\"\s*:\s*\{(?P<body>[^}]+)\}",
	re.IGNORECASE,
)


def _extract_powerreviews_config_from_html(html: str) -> Optional[PowerReviewsConfig]:
	# The PDP includes a JSON-like blob inside script tags. Sometimes quotes are escaped (\"),
	# sometimes they are not. We parse flexibly by extracting the object body first.
	m = _POWERREVIEWS_CONFIG_RE.search(html)
	if not m:
		return None

	body = m.group("body")
	mid = re.search(r"\\?\"merchantId\\?\"\s*:\s*(\d+)", body, re.IGNORECASE)
	key = re.search(r"\\?\"apiKey\\?\"\s*:\s*\\?\"([^\"\\]+)", body, re.IGNORECASE)
	if not mid or not key:
		return None
	return PowerReviewsConfig(merchant_id=mid.group(1), api_key=key.group(1))


def discover_powerreviews_config(
	*,
	sample_pid: str,
	session: requests.Session,
	locale: str,
) -> PowerReviewsConfig:
	# Pull config from PDP HTML; this avoids hard-coding merchant/apiKey.
	url = f"{GAP_BASE}/browse/product.do?pid={sample_pid}"
	resp = session.get(url, headers={"Accept": "text/html,*/*"}, timeout=30)
	resp.raise_for_status()
	html = resp.text

	cfg = _extract_powerreviews_config_from_html(html)
	if not cfg:
		raise RuntimeError("Unable to find powerReviewsConfig on PDP")
	return dataclasses.replace(cfg, locale=locale)


def iter_powerreviews_reviews(
	*,
	pr: PowerReviewsConfig,
	style_id: str,
	session: requests.Session,
	request_sleep_s: float,
) -> Iterable[Dict[str, Any]]:
	base = f"https://display.powerreviews.com/m/{pr.merchant_id}/l/{pr.locale}/product/{style_id}/reviews"
	paging_from = 0
	paging_size = 25  # enforced maximum

	while True:
		params = {
			"apikey": pr.api_key,
			"paging.from": paging_from,
			"paging.size": paging_size,
			"sort": "Newest",
			"page_locale": pr.locale,
		}
		last_exc: Optional[BaseException] = None
		for attempt in range(1, 6):
			try:
				resp = session.get(base, params=params, headers={"Accept": "application/json"}, timeout=30)
				# Retry on rate limiting / transient server errors.
				if resp.status_code in {429, 500, 502, 503, 504}:
					raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
				resp.raise_for_status()
				data = resp.json()
				last_exc = None
				break
			except (requests.Timeout, requests.ConnectionError, requests.HTTPError, json.JSONDecodeError) as e:
				last_exc = e
				# Exponential backoff with a small floor.
				sleep_s = min(10.0, 0.5 * (2 ** (attempt - 1)))
				time.sleep(sleep_s)
		if last_exc is not None:
			raise last_exc

		paging = data.get("paging") or {}
		total_results = int(paging.get("total_results") or 0)
		results = data.get("results")
		if not isinstance(results, list) or not results:
			break
		page = results[0]
		reviews = page.get("reviews") or []
		if not reviews:
			break

		for review in reviews:
			yield review

		paging_from += len(reviews)
		if paging_from >= total_results:
			break

		if request_sleep_s:
			time.sleep(request_sleep_s)


def _load_pr_cache(path: str) -> Dict[str, Tuple[int, Dict[int, int], int]]:
	try:
		with open(path, "r", encoding="utf-8") as f:
			data = json.load(f)
	except FileNotFoundError:
		return {}
	except Exception:
		return {}
	if not isinstance(data, dict):
		return {}
	out: Dict[str, Tuple[int, Dict[int, int], int]] = {}
	for style_id, rec in data.items():
		if not isinstance(style_id, str) or not isinstance(rec, dict):
			continue
		last12 = rec.get("last12")
		years = rec.get("years")
		total = rec.get("total")
		if not isinstance(last12, int) or not isinstance(total, int) or not isinstance(years, dict):
			continue
		year_counts: Dict[int, int] = {}
		ok = True
		for y in (2020, 2021, 2022, 2023, 2024, 2025):
			v = years.get(str(y)) if str(y) in years else years.get(y)
			if not isinstance(v, int):
				ok = False
				break
			year_counts[y] = v
		if not ok:
			continue
		out[style_id] = (last12, year_counts, total)
	return out


def _save_pr_cache(path: str, cache: Dict[str, Tuple[int, Dict[int, int], int]]) -> None:
	# Atomic-ish write to avoid corrupting cache on interruption.
	tmp = f"{path}.tmp"
	data: Dict[str, Any] = {}
	for style_id, (last12, year_counts, total) in cache.items():
		data[style_id] = {
			"last12": int(last12),
			"total": int(total),
			"years": {str(y): int(c) for y, c in year_counts.items()},
		}
	with open(tmp, "w", encoding="utf-8") as f:
		json.dump(data, f)
	os.replace(tmp, path)


def compute_review_counts(
	*,
	pr: PowerReviewsConfig,
	style_id: str,
	session: requests.Session,
	now_utc: datetime,
	request_sleep_s: float,
) -> Tuple[int, Dict[int, int], int]:
	last12_start = now_utc - timedelta(days=365)
	year_counts = {2020: 0, 2021: 0, 2022: 0, 2023: 0, 2024: 0, 2025: 0}
	last12 = 0
	total = 0

	for review in iter_powerreviews_reviews(
		pr=pr,
		style_id=style_id,
		session=session,
		request_sleep_s=request_sleep_s,
	):
		total += 1
		details = review.get("details") or {}
		created_ms = details.get("created_date")
		if created_ms is None:
			continue

		try:
			created_dt = datetime.fromtimestamp(int(created_ms) / 1000, tz=timezone.utc)
		except Exception:
			continue

		if created_dt >= last12_start:
			last12 += 1

		y = created_dt.year
		if y in year_counts:
			year_counts[y] += 1

	return last12, year_counts, total


def build_rows(
	*,
	cid: Optional[str],
	locale: str,
	category_url: str,
	session: requests.Session,
	max_products: Optional[int],
	gap_page_sleep_s: float,
	pr_request_sleep_s: float,
	granularity: str,
	reviews_mode: str,
	search_name_filter: bool,
	pr_cache_path: Optional[str],
	progress_every: int,
) -> List[ProductRow]:
	extra = _parse_params_from_gap_url(category_url)
	use_style_api = _gap_products_api_url(cid=cid, extra_params=extra) == GAP_PRODUCTS_STYLE_API
	products = fetch_gap_products(
		cid=cid,
		locale=locale,
		referer_url=category_url,
		session=session,
		page_sleep_s=gap_page_sleep_s,
		extra_params=extra,
	)

	# For keyword searches, the raw API can include adjacent items (e.g., matching a collection/line)
	# that do not include the keyword in the product name. When enabled, filter to name matches.
	keyword = (extra.get("keyword") or "").strip()
	if search_name_filter and use_style_api and keyword:
		kw_tokens = [t for t in re.findall(r"[a-z0-9]+", keyword.lower()) if t]
		if kw_tokens:
			filtered: List[Dict[str, Any]] = []
			for p in products:
				name_l = _style_name_from_product(p).lower()
				if all(t in name_l for t in kw_tokens):
					filtered.append(p)
			products = filtered

	if max_products is not None:
		products = products[: max_products]

	if not products:
		return []

	if reviews_mode not in {"powerreviews", "gap-only"}:
		raise ValueError("reviews_mode must be one of: powerreviews, gap-only")

	pr: Optional[PowerReviewsConfig] = None
	if reviews_mode == "powerreviews":
		# Discover PR config from first product's first color id (pid/ccId)
		colors0 = _colors_from_product(products[0])
		sample_pid = _first_ccid(colors0)
		if not sample_pid:
			raise RuntimeError("Unable to find a sample product pid (ccId) from product list")
		pr = discover_powerreviews_config(sample_pid=sample_pid, session=session, locale=locale)

	now_utc = datetime.now(timezone.utc)
	rows: List[ProductRow] = []
	style_review_cache: Dict[str, Tuple[int, Dict[int, int], int]] = {}
	if reviews_mode == "powerreviews" and pr_cache_path:
		style_review_cache.update(_load_pr_cache(pr_cache_path))

	# Auto mode: if totalColors is much higher than style count, expand to color rows.
	# This matches large grids like GapBody (705+ items).
	if granularity not in {"auto", "style", "color"}:
		raise ValueError("granularity must be one of: auto, style, color")
	use_color_rows: bool
	if use_style_api:
		# Search pages return "items" (often one per color), so auto should match the grid.
		use_color_rows = (granularity in {"auto", "color"})
	elif granularity == "style":
		use_color_rows = False
	elif granularity == "color":
		use_color_rows = True
	else:
		# best-effort heuristic
		api_url = _gap_products_api_url(cid=cid, extra_params=extra)
		use_style_api = api_url == GAP_PRODUCTS_STYLE_API
		base_params: Dict[str, str] = {"locale": locale, "pageNumber": "0"}
		if cid and not use_style_api:
			base_params["cid"] = str(cid)
		if use_style_api:
			base_params["pageSize"] = "200"
		base_params.update({k: v for k, v in extra.items() if k not in {"cid", "locale", "pageNumber", "pageId"}})
		page0 = session.get(
			api_url,
			params=base_params,
			headers={"Referer": category_url},
			timeout=30,
		).json()
		total_colors = _safe_int(page0.get("totalColors"))
		use_color_rows = bool(total_colors and total_colors > len(products))

	# For the search (style) API, each product entry typically represents a single grid item.
	# In style granularity, we group these back into unique styles.
	if use_style_api and not use_color_rows:
		ordered_style_ids: List[str] = []
		style_agg: Dict[str, Dict[str, Any]] = {}
		for p in products:
			style_id = _style_id_from_product(p)
			if not style_id:
				continue
			colors = _colors_from_product(p)
			first_color = colors[0] if colors else {}
			pid = first_color.get("id") or first_color.get("ccId")
			pid_s = str(pid) if pid else ""
			price = _price_from_style_color(first_color) if first_color else None
			agg = style_agg.get(style_id)
			if agg is None:
				ordered_style_ids.append(style_id)
				agg = {
					"name": _style_name_from_product(p),
					"rating": _safe_float(p.get("reviewScore")),
					"reviews_count": _safe_int(p.get("reviewCount")),
					"min_price": price,
					"pid": pid_s,
				}
				style_agg[style_id] = agg
			else:
				if agg.get("min_price") is None or (price is not None and price < agg.get("min_price")):
					agg["min_price"] = price
				if not agg.get("pid") and pid_s:
					agg["pid"] = pid_s

		rows: List[ProductRow] = []
		style_review_cache: Dict[str, Tuple[int, Dict[int, int], int]] = {}
		seen_styles = set()
		for i, style_id in enumerate(ordered_style_ids, start=1):
			if style_id in seen_styles:
				continue
			seen_styles.add(style_id)
			agg = style_agg.get(style_id) or {}
			full_name = str(agg.get("name") or "").strip()
			rating = agg.get("rating")
			reviews_count = agg.get("reviews_count")
			pid_s = str(agg.get("pid") or "")
			product_url = f"{GAP_BASE}/browse/product.do?pid={pid_s}" if pid_s else ""
			price = agg.get("min_price")

			last12: Optional[int]
			year_counts: Dict[int, int]
			pr_total: Optional[int]
			if reviews_mode == "powerreviews":
				if pr is None:
					raise RuntimeError("PowerReviews config missing")
				if style_id not in style_review_cache:
					last12_i, year_counts_i, pr_total_i = compute_review_counts(
						pr=pr,
						style_id=style_id,
						session=session,
						now_utc=now_utc,
						request_sleep_s=pr_request_sleep_s,
					)
					style_review_cache[style_id] = (last12_i, year_counts_i, pr_total_i)
					if pr_cache_path:
						_save_pr_cache(pr_cache_path, style_review_cache)
				else:
					last12_i, year_counts_i, pr_total_i = style_review_cache[style_id]
				last12 = last12_i
				year_counts = year_counts_i
				pr_total = pr_total_i
			else:
				last12 = None
				year_counts = {2020: 0, 2021: 0, 2022: 0, 2023: 0, 2024: 0, 2025: 0}
				pr_total = None

			if reviews_count is None and pr_total is not None:
				reviews_count = pr_total

			rows.append(
				ProductRow(
					brand_name="Gap",
					full_name=full_name,
					price=price,
					product_rating=rating,
					customer_reviews_count=reviews_count,
					product_url=product_url,
					customer_reviews_last_12_months_count=last12,
					customer_reviews_2020_count=year_counts[2020],
					customer_reviews_2021_count=year_counts[2021],
					customer_reviews_2022_count=year_counts[2022],
					customer_reviews_2023_count=year_counts[2023],
					customer_reviews_2024_count=year_counts[2024],
					customer_reviews_2025_count=year_counts[2025],
				)
			)
			if progress_every > 0 and (i == 1 or i == len(ordered_style_ids) or i % progress_every == 0):
				print(f"[{i}/{len(ordered_style_ids)}] {full_name} | reviews={reviews_count} | price={price}")

		return rows

	seen_style_ids: set[str] = set()
	seen_pids: set[str] = set()
	for i, p in enumerate(products, start=1):
		style_id = _style_id_from_product(p)
		if not style_id:
			continue
		full_name = _style_name_from_product(p)
		style_colors = _colors_from_product(p)

		rating = _safe_float(p.get("reviewScore"))

		last12: Optional[int]
		year_counts: Dict[int, int]
		pr_total: Optional[int]
		if reviews_mode == "powerreviews":
			if pr is None:
				raise RuntimeError("PowerReviews config missing")
			if style_id not in style_review_cache:
				last12_i, year_counts_i, pr_total_i = compute_review_counts(
					pr=pr,
					style_id=style_id,
					session=session,
					now_utc=now_utc,
					request_sleep_s=pr_request_sleep_s,
				)
				style_review_cache[style_id] = (last12_i, year_counts_i, pr_total_i)
				if pr_cache_path:
					_save_pr_cache(pr_cache_path, style_review_cache)
			else:
				last12_i, year_counts_i, pr_total_i = style_review_cache[style_id]
			last12 = last12_i
			year_counts = year_counts_i
			pr_total = pr_total_i
		else:
			last12 = None
			year_counts = {2020: 0, 2021: 0, 2022: 0, 2023: 0, 2024: 0, 2025: 0}
			pr_total = None

		# Use Gap's reviewCount as the overall count shown on the category/product UI.
		# PowerReviews paging.total_results counts written reviews; Gap's value can be higher
		# because it may include star ratings without review text.
		reviews_count = _safe_int(p.get("reviewCount"))
		if reviews_count is None and pr_total is not None:
			reviews_count = pr_total

		if use_color_rows:
			# For search API items, treat each product entry as a single grid item (often one color).
			if use_style_api:
				first_color = style_colors[0] if style_colors else {}
				pid = first_color.get("id") or first_color.get("ccId")
				if not pid:
					continue
				pid_s = str(pid)
				if pid_s in seen_pids:
					continue
				seen_pids.add(pid_s)
				product_url = f"{GAP_BASE}/browse/product.do?pid={pid_s}"
				cc_name = (first_color.get("ccName") or first_color.get("name") or "").strip()
				display_name = full_name
				if cc_name:
					display_name = f"{full_name} - {cc_name}"
				price = _price_from_style_color(first_color)
				rows.append(
					ProductRow(
						brand_name="Gap",
						full_name=display_name,
						price=price,
						product_rating=rating,
						customer_reviews_count=reviews_count,
						product_url=product_url,
						customer_reviews_last_12_months_count=last12,
						customer_reviews_2020_count=year_counts[2020],
						customer_reviews_2021_count=year_counts[2021],
						customer_reviews_2022_count=year_counts[2022],
						customer_reviews_2023_count=year_counts[2023],
						customer_reviews_2024_count=year_counts[2024],
						customer_reviews_2025_count=year_counts[2025],
					)
				)
				if progress_every > 0 and (i == 1 or i == len(products) or i % progress_every == 0):
					print(f"[{i}/{len(products)}] {display_name} | reviews={reviews_count} | price={price}")
				continue

			# Category API: expand each style into color rows.
			seen_ccids: set[str] = set()
			for sc in style_colors:
				ccid = sc.get("ccId") or sc.get("id")
				if not ccid:
					continue
				ccid_s = str(ccid)
				if ccid_s in seen_ccids:
					continue
				seen_ccids.add(ccid_s)
				product_url = f"{GAP_BASE}/browse/product.do?pid={ccid_s}"
				cc_name = (sc.get("ccName") or sc.get("name") or "").strip()
				display_name = full_name
				if cc_name:
					display_name = f"{full_name} - {cc_name}"
				price = _price_from_style_color(sc)
				rows.append(
					ProductRow(
						brand_name="Gap",
						full_name=display_name,
						price=price,
						product_rating=rating,
						customer_reviews_count=reviews_count,
						product_url=product_url,
						customer_reviews_last_12_months_count=last12,
						customer_reviews_2020_count=year_counts[2020],
						customer_reviews_2021_count=year_counts[2021],
						customer_reviews_2022_count=year_counts[2022],
						customer_reviews_2023_count=year_counts[2023],
						customer_reviews_2024_count=year_counts[2024],
						customer_reviews_2025_count=year_counts[2025],
					)
				)
			if progress_every > 0 and (i == 1 or i == len(products) or i % progress_every == 0):
				print(f"[{i}/{len(products)}] {full_name} | style={style_id} | colors={len(style_colors)}")
		else:
			if style_id in seen_style_ids:
				continue
			seen_style_ids.add(style_id)
			ccid = _first_ccid(style_colors)
			product_url = f"{GAP_BASE}/browse/product.do?pid={ccid}" if ccid else ""
			price = _min_price_from_style_colors(style_colors)
			rows.append(
				ProductRow(
					brand_name="Gap",
					full_name=full_name,
					price=price,
					product_rating=rating,
					customer_reviews_count=reviews_count,
					product_url=product_url,
					customer_reviews_last_12_months_count=last12,
					customer_reviews_2020_count=year_counts[2020],
					customer_reviews_2021_count=year_counts[2021],
					customer_reviews_2022_count=year_counts[2022],
					customer_reviews_2023_count=year_counts[2023],
					customer_reviews_2024_count=year_counts[2024],
					customer_reviews_2025_count=year_counts[2025],
				)
			)
			if progress_every > 0 and (i == 1 or i == len(products) or i % progress_every == 0):
				print(f"[{i}/{len(products)}] {full_name} | reviews={reviews_count} | price={price}")

	return rows


def write_csv(rows: List[ProductRow], out_path: str, *, excel_hyperlinks: bool = False) -> None:
	fieldnames = [
		"Brand Name",
		"Full Name",
		"Price",
		"Product Rating",
		"Customer Reviews Count",
		"Product URL",
		"Customer Reviews (Last 12 Months) Count",
		"Customer Reviews 2020 Count",
		"Customer Reviews 2021 Count",
		"Customer Reviews 2022 Count",
		"Customer Reviews 2023 Count",
		"Customer Reviews 2024 Count",
		"Customer Reviews 2025 Count",
	]
	with open(out_path, "w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=fieldnames)
		w.writeheader()
		for r in rows:
			url_cell = r.product_url
			if excel_hyperlinks and url_cell:
				# Excel will interpret this as a clickable hyperlink when opening the CSV.
				url_cell = f'=HYPERLINK("{url_cell}","{url_cell}")'
			w.writerow(
				{
					"Brand Name": r.brand_name,
					"Full Name": r.full_name,
					"Price": ("" if r.price is None else f"{r.price:.2f}"),
					"Product Rating": ("" if r.product_rating is None else f"{r.product_rating:.2f}"),
					"Customer Reviews Count": ("" if r.customer_reviews_count is None else str(r.customer_reviews_count)),
					"Product URL": url_cell,
					"Customer Reviews (Last 12 Months) Count": (
						"" if r.customer_reviews_last_12_months_count is None else str(r.customer_reviews_last_12_months_count)
					),
					"Customer Reviews 2020 Count": ("" if r.customer_reviews_2020_count is None else str(r.customer_reviews_2020_count)),
					"Customer Reviews 2021 Count": ("" if r.customer_reviews_2021_count is None else str(r.customer_reviews_2021_count)),
					"Customer Reviews 2022 Count": ("" if r.customer_reviews_2022_count is None else str(r.customer_reviews_2022_count)),
					"Customer Reviews 2023 Count": ("" if r.customer_reviews_2023_count is None else str(r.customer_reviews_2023_count)),
					"Customer Reviews 2024 Count": ("" if r.customer_reviews_2024_count is None else str(r.customer_reviews_2024_count)),
					"Customer Reviews 2025 Count": ("" if r.customer_reviews_2025_count is None else str(r.customer_reviews_2025_count)),
				}
			)


def parse_args() -> argparse.Namespace:
	p = argparse.ArgumentParser(description="Scrape Gap category products + review counts into CSV")
	p.add_argument(
		"--url",
		default=(
			"https://www.gap.com/browse/women/gapbody?cid=1140272"
			"#pageId=0&department=136&mlink=5643,20012060,DP_1_W_LoveByGap"
		),
		help="Gap category URL (must include ?cid=... or pass --cid)",
	)
	p.add_argument(
		"--cid",
		default=None,
		help="Optional category id (cid). If omitted, extracted from --url.",
	)
	p.add_argument("--locale", default="en_US", help="Locale")
	p.add_argument("--out", default="gap_women_gapbody_1140272.csv", help="Output CSV path")
	p.add_argument(
		"--granularity",
		choices=["auto", "style", "color"],
		default="style",
		help=(
			"Row granularity: style (one row per styleId) or color (one row per ccId). "
			"auto chooses based on totalColors."
		),
	)
	p.add_argument(
		"--excel-hyperlinks",
		action="store_true",
		help='Write Product URL as an Excel HYPERLINK() formula so links are clickable when opening the CSV in Excel.',
	)
	p.add_argument(
		"--reviews",
		choices=["powerreviews", "gap-only"],
		default="powerreviews",
		help=(
			"Review mode: powerreviews (slow; computes year/last-12-month counts) "
			"or gap-only (fast; uses Gap's category/search reviewCount only)."
		),
	)
	p.add_argument(
		"--no-search-name-filter",
		action="store_true",
		help=(
			"Disable strict name filtering for keyword searches. By default, when using a search URL "
			"(browse/search.do?searchText=...), results are filtered to product names that contain the keyword."
		),
	)
	p.add_argument(
		"--pr-cache",
		default="powerreviews_cache.json",
		help=(
			"Path to a JSON cache for PowerReviews counts. Helps resume long runs without losing progress. "
			"Set to empty string to disable."
		),
	)
	p.add_argument(
		"--max-products",
		type=int,
		default=None,
		help="Optional limit for faster runs (e.g. 5)",
	)
	p.add_argument(
		"--progress-every",
		type=int,
		default=10,
		help="Print progress every N products (0 disables).",
	)
	p.add_argument(
		"--gap-page-sleep",
		type=float,
		default=0.3,
		help="Sleep between Gap product-list page requests (seconds)",
	)
	p.add_argument(
		"--pr-sleep",
		type=float,
		default=0.2,
		help="Sleep between PowerReviews page requests (seconds)",
	)
	return p.parse_args()


def main() -> int:
	args = parse_args()
	session = _requests_session()

	url_params = _parse_params_from_gap_url(str(args.url))
	# Gap search pages use browse/search.do?searchText=... but the products API expects keyword=...
	if "searchText" in url_params and "keyword" not in url_params:
		url_params["keyword"] = url_params["searchText"]
		url_params.pop("searchText", None)

	cid = str(args.cid) if args.cid else url_params.get("cid")
	keyword = url_params.get("keyword")
	if not cid and not keyword:
		raise SystemExit(
			"Missing cid/keyword: pass --cid, or use a category URL with ?cid=..., or a search URL like "
			"https://www.gap.com/browse/search.do?searchText=bra. "
			f"Got --url={args.url!s}"
		)

	rows = build_rows(
		cid=cid,
		locale=str(args.locale),
		category_url=str(args.url),
		session=session,
		max_products=args.max_products,
		gap_page_sleep_s=float(args.gap_page_sleep),
		pr_request_sleep_s=float(args.pr_sleep),
		granularity=str(args.granularity),
		reviews_mode=str(args.reviews),
		search_name_filter=(not bool(args.no_search_name_filter)),
		pr_cache_path=(None if str(args.pr_cache).strip() == "" else str(args.pr_cache)),
		progress_every=int(args.progress_every),
	)
	write_csv(rows, str(args.out), excel_hyperlinks=bool(args.excel_hyperlinks))
	print(f"Wrote {len(rows)} rows to {args.out}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

