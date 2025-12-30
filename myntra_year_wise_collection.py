from playwright.sync_api import sync_playwright
import time, random
from openpyxl import Workbook
import re
import csv
import os
from datetime import datetime, timedelta


def scrape_mns_myntra():
    # Add one or more listing URLs here. All products from all URLs will be saved
    # into the same CSV and the same Excel sheet.
    urls = [
        "https://www.myntra.com/jockey-underwear?f=Color%3ACharcoal_36454f&rawQuery=jockey%20underwear%20",
        "https://www.myntra.com/jockey-bra?f=Color%3ABlack_36454f&rawQuery=jockey%20bra%20",
    ]

    csv_name = "myntra_h_and_m_bodysuit.csv"
    xlsx_name = "myntra_h_and_m_bodysuit.xlsx"

    csv_headers = [
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

    results = []

    if (not os.path.exists(csv_name)) or (os.path.exists(csv_name) and os.path.getsize(csv_name) == 0):
        with open(csv_name, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(csv_headers)

    def _is_myntra_oops_page(p) -> bool:
        try:
            # Myntra error page typically renders: "Oops! Something went wrong." + a "Refresh" button.
            if p.locator(r"text=/Oops!?\s*Something\s*went\s*wrong\.?/i").count() > 0:
                return True
            if p.locator(r"text=/Something\s+went\s+wrong/i").count() > 0 and p.locator(r"text=/Refresh/i").count() > 0:
                return True
        except Exception:
            return False
        return False

    def _recover_from_myntra_oops(p, target_url: str | None = None, max_attempts: int = 6) -> bool:
        # Best-effort recovery: click Refresh if present, then reload/go-to the target URL.
        # Returns True if page no longer looks like the oops page.
        target_url = (target_url or "").strip() or None
        for _ in range(max_attempts):
            if not _is_myntra_oops_page(p):
                return True
            try:
                btn = p.locator(":is(button,a,div,span):has-text('Refresh'), text=/^Refresh$/i").first
                if btn and btn.count() > 0:
                    try:
                        btn.scroll_into_view_if_needed(timeout=1500)
                    except Exception:
                        pass
                    try:
                        btn.click(timeout=2500)
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                # Manual refresh typically re-navigates to the same URL; do the same when we can.
                if target_url:
                    p.goto(target_url, timeout=90000, wait_until="domcontentloaded")
                else:
                    p.reload(timeout=90000, wait_until="domcontentloaded")
            except Exception:
                pass

            try:
                p.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            time.sleep(0.7 + random.uniform(0.15, 0.45))
        return not _is_myntra_oops_page(p)

    def _wait_for_listing_products(p, timeout_ms: int = 60000) -> bool:
        # Waits for listing cards while auto-recovering from Myntra error pages.
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        while time.monotonic() < deadline:
            if _is_myntra_oops_page(p):
                _recover_from_myntra_oops(p, target_url=p.url, max_attempts=6)
            try:
                n = p.evaluate("() => document.querySelectorAll('li.product-base').length")
                if isinstance(n, int) and n > 0:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def _wait_for_pdp_ready(p, timeout_ms: int = 25000) -> bool:
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        while time.monotonic() < deadline:
            if _is_myntra_oops_page(p):
                _recover_from_myntra_oops(p, target_url=p.url, max_attempts=6)
            try:
                if p.locator("h1.pdp-name, h1.pdp-title, div.pdp-title h1").count() > 0:
                    return True
            except Exception:
                pass
            time.sleep(0.35)
        return False

    def _wait_for_reviews_ready(p, timeout_ms: int = 25000) -> bool:
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        while time.monotonic() < deadline:
            if _is_myntra_oops_page(p):
                _recover_from_myntra_oops(p, target_url=p.url, max_attempts=6)
            try:
                # Any review card/date element or sort control is enough.
                if p.locator("div[itemprop='review'], div.pdp-review, div.review, .reviewCard, .user-review, li.review").count() > 0:
                    return True
                if p.locator(r"text=/Sort\s*by/i").count() > 0:
                    return True
            except Exception:
                pass
            time.sleep(0.35)
        return False

    def _parse_review_date(date_text: str):
        if not date_text:
            return None
        s = re.sub(r"\s+", " ", date_text).strip()
        s = s.replace(",", " ")
        s = re.sub(r"\bSept\b", "Sep", s, flags=re.I)

        now = datetime.now()

        m_tail = re.search(r"(\d{1,2})\s*([A-Za-z]{3,9})\s*(\d{2,4})\s*$", s)
        if m_tail:
            day = int(m_tail.group(1))
            mon = m_tail.group(2)
            if mon.lower() == "sept":
                mon = "Sep"
            year = int(m_tail.group(3))
            if year < 100:
                year += 2000
            for fmt in ("%d %b %Y", "%d %B %Y"):
                try:
                    return datetime.strptime(f"{day} {mon} {year}", fmt)
                except Exception:
                    pass

        if re.fullmatch(r"(?i)today", s):
            return now
        if re.fullmatch(r"(?i)yesterday", s):
            return now - timedelta(days=1)

        m_rel = re.search(r"(?i)\b(\d+)\s*(day|days|month|months|year|years)\s*ago\b", s)
        if m_rel:
            n = int(m_rel.group(1))
            unit = m_rel.group(2).lower()
            if unit.startswith("day"):
                return now - timedelta(days=n)
            if unit.startswith("month"):
                return now - timedelta(days=30 * n)
            if unit.startswith("year"):
                return now - timedelta(days=365 * n)

        m_abs = re.search(r"\b(\d{1,2})\s*([A-Za-z]{3,9})\s*(\d{2,4})\b", s)
        if m_abs:
            day = int(m_abs.group(1))
            mon = m_abs.group(2)
            if mon.lower() == "sept":
                mon = "Sep"
            year = int(m_abs.group(3))
            if year < 100:
                year += 2000
            for fmt in ("%d %b %Y", "%d %B %Y"):
                try:
                    return datetime.strptime(f"{day} {mon} {year}", fmt)
                except Exception:
                    pass

        m_dm = re.search(r"\b(\d{1,2})\s*([A-Za-z]{3,9})\b", s)
        if m_dm:
            day = int(m_dm.group(1))
            mon = m_dm.group(2)
            if mon.lower() == "sept":
                mon = "Sep"
            year = datetime.now().year
            for fmt in ("%d %b %Y", "%d %B %Y"):
                try:
                    dt = datetime.strptime(f"{day} {mon} {year}", fmt)
                    if dt > datetime.now() + timedelta(days=2):
                        dt = datetime(dt.year - 1, dt.month, dt.day)
                    return dt
                except Exception:
                    pass

        m_my = re.search(r"\b([A-Za-z]{3,9})\s*(\d{4})\b", s)
        if m_my:
            mon = m_my.group(1)
            year = int(m_my.group(2))
            for fmt in ("%b %Y", "%B %Y"):
                try:
                    dt = datetime.strptime(f"{mon} {year}", fmt)
                    return datetime(dt.year, dt.month, 1)
                except Exception:
                    pass

        m_num = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b", s)
        if m_num:
            d = int(m_num.group(1))
            m = int(m_num.group(2))
            y = int(m_num.group(3))
            if y < 100:
                y += 2000
            try:
                return datetime(y, m, d)
            except Exception:
                return None

        return None

    def _scroll_reviews(dp):
        try:
            scrolled = dp.evaluate(
                """() => {
                    const pickScrollable = () => {
                      const preferred = [
                        '[class*=review i]',
                        '[class*=detailed i]',
                        '[class*=ratings i]'
                      ];
                      const all = Array.from(document.querySelectorAll('div, section, main, ul'));
                      const candidates = all.filter(el => {
                        const st = window.getComputedStyle(el);
                        const oy = st.overflowY;
                        return (oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 200;
                      });
                      candidates.sort((a,b) => {
                        const as = a.className || '';
                        const bs = b.className || '';
                        const ap = preferred.some(s => a.matches && a.matches(s)) || /review|rating|detailed/i.test(as);
                        const bp = preferred.some(s => b.matches && b.matches(s)) || /review|rating|detailed/i.test(bs);
                        if (ap && !bp) return -1;
                        if (!ap && bp) return 1;
                        return (b.scrollHeight-b.clientHeight) - (a.scrollHeight-a.clientHeight);
                      });
                      return candidates[0] || null;
                    };

                    const step = (el) => Math.max(600, Math.floor(el.clientHeight * 0.85));
                    const el = pickScrollable();
                    if (el) {
                      const before = el.scrollTop;
                      const next = Math.min(el.scrollTop + step(el), el.scrollHeight);
                      el.scrollTop = next;
                      return el.scrollTop !== before;
                    }
                    const before = window.scrollY;
                    window.scrollBy(0, Math.max(700, Math.floor(window.innerHeight * 0.85)));
                    return window.scrollY !== before;
                }"""
            )
            if scrolled:
                return True
        except Exception:
            pass
        try:
            dp.mouse.wheel(0, 1400)
            return True
        except Exception:
            return False

    def _extract_reviews_with_dates(dp):
        try:
            return dp.evaluate(
                r"""() => {
                  const reviewSelectors = [
                    "div[itemprop='review']",
                    "div.pdp-review",
                    "div.review",
                    ".reviewCard",
                    ".user-review",
                    "div[class*='user-review' i]",
                    "div[class*='userReview' i]",
                    "li.review",
                    "div[class*='reviewCard' i]",
                    "div[class*='review' i]"
                  ];
                  const reviewSel = reviewSelectors.join(',');
                  const uniq = new Set();

                  const pickDateFromText = (raw) => {
                    const r1y4 = raw.match(/(\d{1,2}\s*[A-Za-z]{3,9}\s*,?\s*\d{4})\b/g);
                    if (r1y4 && r1y4.length) return r1y4[r1y4.length - 1];
                    const r1y2 = raw.match(/(\d{1,2}\s*[A-Za-z]{3,9}\s*,?\s*\d{2})\b/g);
                    if (r1y2 && r1y2.length) return r1y2[r1y2.length - 1];
                    const rmy = raw.match(/([A-Za-z]{3,9}\s*\d{4})\b/g);
                    if (rmy && rmy.length) return rmy[rmy.length - 1];
                    const rdm = raw.match(/(\d{1,2}\s*[A-Za-z]{3,9})\b/g);
                    if (rdm && rdm.length) return rdm[rdm.length - 1];
                    const r2 = raw.match(/(\d+\s*(?:day|days|month|months|year|years)\s*ago)\b/ig);
                    if (r2 && r2.length) return r2[r2.length - 1];
                    return "";
                  };

                  const normalize = (s) => (s || '').replace(/\s+/g, ' ').trim();

                  const djb2 = (str) => {
                    let h = 5381;
                    for (let i = 0; i < str.length; i++) {
                      h = ((h << 5) + h) ^ str.charCodeAt(i);
                    }
                    return (h >>> 0).toString(16);
                  };

                  const pickReviewId = (el) => {
                    if (!el || !el.getAttribute) return "";
                    const attrs = ['data-reviewid','data-review-id','data-reviewId','data-id','data-uuid','data-review','id'];
                    for (const a of attrs) {
                      const v = (el.getAttribute(a) || '').trim();
                      if (!v) continue;
                      if (/\d{6,}/.test(v) || /[a-f0-9]{8,}/i.test(v)) return v;
                    }
                    return "";
                  };

                  const makeKey = (containerEl, containerText, dateText) => {
                    const d = normalize(dateText);
                    if (!d) return "";
                    const rid = pickReviewId(containerEl);
                    if (rid) return d + '|id:' + rid;
                    const t = normalize(containerText);
                    return d + '|h:' + djb2(t);
                  };

                  const out = [];

                  const pushReview = (dateText, containerEl) => {
                    const dt = normalize(dateText);
                    if (!dt) return;
                    let c = containerEl;
                    try {
                      if (c && c.closest) {
                        const cc = c.closest(reviewSel);
                        if (cc) c = cc;
                      }
                    } catch(e) {}
                    const full = (c && (c.innerText || c.textContent)) || '';
                    const key = makeKey(c, full, dt);
                    if (!key || key.length < 8) return;
                    if (uniq.has(key)) return;
                    uniq.add(key);
                    out.push({ key, dateText: dt });
                  };

                  const dateCandidates = Array.from(document.querySelectorAll('time, span, div, p, li')).slice(0, 20000);
                  const dateNodes = [];
                  for (const el of dateCandidates) {
                    const t = normalize(el.textContent);
                    if (!t) continue;
                    if (!/(\d{1,2}\s*[A-Za-z]{3,9}\s*\d{2,4})\b/.test(t) && !/(\d+\s*(?:day|days|month|months|year|years)\s*ago)\b/i.test(t)) continue;
                    const dt = pickDateFromText(t);
                    if (!dt) continue;
                    dateNodes.push({ el, dt });
                    if (dateNodes.length >= 1200) break;
                  }

                  for (const dn of dateNodes) {
                    const dt = dn.dt;
                    const dtCompact = (dt || '').replace(/\s+/g, '').toLowerCase();
                    let container = dn.el;
                    try {
                      if (container && container.closest) {
                        const cc = container.closest(reviewSel);
                        if (cc) container = cc;
                      }
                    } catch(e) {}
                    for (let i = 0; i < 8; i++) {
                      const p = container.parentElement;
                      if (!p) break;
                      const pt = normalize(p.innerText || p.textContent);
                      const ct = normalize(container.innerText || container.textContent);
                      if (!pt || pt.length < ct.length + 15) break;
                      if (pt.length > 2000) break;
                      const ptCompact = pt.replace(/\s+/g, '').toLowerCase();
                      if (dtCompact && ptCompact.indexOf(dtCompact) === -1) break;
                      container = p;
                    }
                    pushReview(dt, container);
                  }

                  if (out.length) return out;

                  for (const sel of reviewSelectors) {
                    const nodes = Array.from(document.querySelectorAll(sel)).slice(0, 2000);
                    for (const node of nodes) {
                      const raw0 = normalize(node.textContent);
                      if (!raw0 || raw0.length < 10) continue;
                      const dateHint = pickDateFromText(raw0);
                      if (!dateHint) continue;
                      let container = node;
                      try {
                        if (container && container.closest) {
                          const cc = container.closest(reviewSel);
                          if (cc) container = cc;
                        }
                      } catch(e) {}
                      const dt = normalize(dateHint);
                      if (!dt) continue;
                      const key = makeKey(container, (container.innerText || container.textContent || ''), dt);
                      if (!key || uniq.has(key)) continue;
                      uniq.add(key);
                      out.push({ key, dateText: dt });
                    }
                  }
                  return out;
                }"""
            )
        except Exception:
            return []

    def _extract_total_reviews_shown(dp):
        try:
            txt = ""
            try:
                a = dp.locator("a.detailed-reviews-allReviews").first
                if a and a.count() > 0:
                    txt = (a.inner_text(timeout=1000) or "").strip()
            except Exception:
                txt = ""

            if not txt:
                try:
                    txt = dp.evaluate(
                        r"""() => {
                            const top = document.body ? (document.body.innerText || '') : '';
                            const head = top.split(/\n+/).slice(0, 60).join(' ');
                            const m = head.match(/\b(\d{1,5})\s+reviews\b/i);
                            return m ? m[1] : '';
                        }"""
                    )
                except Exception:
                    txt = ""

            if not txt:
                return None
            m = re.search(r"(\d{1,5})\s+reviews", txt, re.I)
            if m:
                return int(m.group(1))
        except Exception:
            return None
        return None

    def _get_reviews_page_url(dp, product_id=None):
        href = ""
        try:
            a = dp.query_selector("a.detailed-reviews-allReviews") or dp.query_selector("a[href^='/reviews/']")
            if a:
                href = (a.get_attribute("href") or "").strip()
        except Exception:
            href = ""

        if not href and product_id:
            href = f"/reviews/{product_id}"

        if href:
            if not href.startswith("http"):
                if not href.startswith("/"):
                    href = "/" + href
                href = "https://www.myntra.com" + href
            return href
        return ""

    def _ensure_reviews_sorted(dp, mode: str):
        mode = (mode or "").strip().lower()
        if mode not in ("helpful", "recent"):
            mode = "helpful"
        try:
            for open_sel in [
                "button:has-text('Sort')",
                "div:has-text('Sort')",
                "span:has-text('Sort')",
                r"text=/Sort\s*by/i",
            ]:
                try:
                    loc = dp.locator(open_sel).first
                    if loc and loc.count() > 0:
                        loc.click(timeout=1500)
                        break
                except Exception:
                    pass

            targets = [r"text=/Most\s+helpful/i", r"text=/Helpful/i"] if mode == "helpful" else [r"text=/Most\s+recent/i", r"text=/Newest/i"]
            for sel in targets:
                try:
                    opt = dp.locator(sel).first
                    if opt and opt.count() > 0:
                        opt.click(timeout=2000)
                        break
                except Exception:
                    pass
        except Exception:
            return

    def _get_review_counts(dp, product_id=None, max_scrolls=180, max_idle=8):
        now = datetime.now()
        window_start = now - timedelta(days=365)
        window_end = now
        years = list(range(2020, 2026))

        reviews_url = _get_reviews_page_url(dp, product_id=product_id)
        if not reviews_url:
            return "", {y: "" for y in years}

        rp = None
        try:
            rp = dp.context.new_page()
            rp.set_default_timeout(15000)
            rp.goto(reviews_url, timeout=45000, wait_until="domcontentloaded")
            try:
                rp.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            # Auto-recover if Myntra shows the "Oops...Refresh" page.
            if _is_myntra_oops_page(rp):
                _recover_from_myntra_oops(rp)
            _wait_for_reviews_ready(rp, timeout_ms=25000)
            _ensure_reviews_sorted(rp, "helpful")
        except Exception:
            try:
                if rp:
                    rp.close()
            except Exception:
                pass
            return "", {y: "" for y in years}

        seen_total = 0
        idle = 0
        start_t = time.monotonic()
        max_seconds = 120

        shown_total = None
        try:
            shown_total = _extract_total_reviews_shown(rp)
        except Exception:
            shown_total = None

        for _ in range(max_scrolls):
            if (time.monotonic() - start_t) > max_seconds:
                break

            try:
                for load_sel in [
                    "button:has-text('Load more')",
                    "a:has-text('Load more')",
                    r"text=/Load\s+more/i",
                    "button:has-text('More reviews')",
                ]:
                    for _clicks in range(2):
                        loc = rp.locator(load_sel).first
                        if not loc or loc.count() <= 0:
                            break
                        try:
                            loc.scroll_into_view_if_needed(timeout=2000)
                            loc.click(timeout=2500)
                            try:
                                rp.wait_for_load_state("networkidle", timeout=5000)
                            except Exception:
                                pass
                            rp.wait_for_timeout(350)
                        except Exception:
                            break
            except Exception:
                pass

            reviews = _extract_reviews_with_dates(rp)
            total = len(reviews)
            if total <= seen_total:
                idle += 1
            else:
                idle = 0
                seen_total = total

            if isinstance(shown_total, int) and shown_total > 0:
                if seen_total >= shown_total and idle >= 1:
                    break

            if idle >= max_idle:
                break

            _scroll_reviews(rp)
            try:
                rp.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            rp.wait_for_timeout(int(450 + random.uniform(75, 175)))

        rolling_last_12m_count = 0
        year_counts = {y: 0 for y in years}
        any_reviews = []
        parsed_any = False
        try:
            any_reviews = _extract_reviews_with_dates(rp)
            for r in any_reviews:
                dt = _parse_review_date(r.get("dateText", ""))
                if not dt:
                    continue
                parsed_any = True
                if window_start <= dt <= window_end:
                    rolling_last_12m_count += 1
                if dt.year in year_counts:
                    year_counts[dt.year] += 1
        except Exception:
            rolling_last_12m_count = 0
            year_counts = {y: 0 for y in years}

        try:
            rp.close()
        except Exception:
            pass

        if not parsed_any and any_reviews:
            return "", {y: "" for y in years}

        return rolling_last_12m_count, year_counts

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
        )

        def should_block(req):
            rt = req.resource_type
            u = req.url
            if rt in ["image", "font", "media", "stylesheet"]:
                return True
            if re.search(r"(doubleclick|google-analytics|adservice|facebook|hotjar|segment|optimizely|ads|tracking)", u, re.I):
                return True
            return False

        context.route("**/*", lambda route: route.abort() if should_block(route.request) else route.continue_())

        def _new_listing_page():
            p2 = context.new_page()
            p2.set_default_timeout(20000)
            return p2

        def _new_detail_page():
            p2 = context.new_page()
            p2.set_default_timeout(15000)
            return p2

        page = _new_listing_page()
        detail_page = _new_detail_page()

        def load_page_with_retries(url, retries=3, delay=2):
            nonlocal page
            for attempt in range(retries):
                try:
                    if page.is_closed():
                        page = _new_listing_page()

                    resp = page.goto(url, timeout=90000, wait_until="domcontentloaded")
                    try:
                        page.wait_for_load_state("networkidle", timeout=45000)
                    except Exception:
                        pass

                    try:
                        if resp is not None and getattr(resp, "status", None) is not None and resp.status >= 400:
                            raise RuntimeError(f"HTTP {resp.status}")
                    except Exception:
                        pass

                    # Auto-recover if Myntra shows the "Oops...Refresh" page.
                    if _is_myntra_oops_page(page):
                        _recover_from_myntra_oops(page, target_url=url, max_attempts=8)
                        # Re-wait for the listing to be ready after recovery.
                        try:
                            page.wait_for_load_state("networkidle", timeout=45000)
                        except Exception:
                            pass

                    if not _wait_for_listing_products(page, timeout_ms=60000):
                        raise RuntimeError("Timed out waiting for product cards")

                    # If we still landed on an error page (or got redirected), force a retry.
                    if _is_myntra_oops_page(page):
                        raise RuntimeError("Myntra returned 'Oops! Something went wrong' page")

                    time.sleep(0.5 + random.uniform(0.0, 0.3))
                    return True
                except Exception as e:
                    print(f"Load attempt {attempt + 1} failed: {e}")
                    try:
                        if not page.is_closed():
                            page.close()
                    except Exception:
                        pass
                    try:
                        page = _new_listing_page()
                    except Exception:
                        pass
                    time.sleep(delay)
            return False

        try:
            max_pages_env = os.environ.get("MYNTRA_MAX_PAGES", "")
            max_pages = int(max_pages_env) if max_pages_env.strip() else 0
            if max_pages < 0:
                max_pages = 0
        except Exception:
            max_pages = 0

        for list_url in (urls or []):
            list_url = (list_url or "").strip()
            if not list_url:
                continue

            print(f"\n Starting URL: {list_url}")
            if not load_page_with_retries(list_url):
                print("Failed to load URL after retries. Skipping.")
                continue

            page_number = 1
            while True:
                print(f" Scraping page {page_number}...")
                products = page.query_selector_all("li.product-base")

                try:
                    max_products = int(os.environ.get("MYNTRA_MAX_PRODUCTS", "0") or "0")
                except Exception:
                    max_products = 0
                if max_products and max_products > 0:
                    products = (products or [])[:max_products]

                for product in products:
                    try:
                        name = product.query_selector("h4.product-product").inner_text().strip()
                    except Exception:
                        name = ""

                    try:
                        brand = product.query_selector("h3.product-brand").inner_text().strip()
                    except Exception:
                        brand = ""

                    try:
                        rating = product.query_selector("div.product-ratingsContainer span").inner_text().strip()
                    except Exception:
                        rating = "0"

                    try:
                        rating_count = product.query_selector("div.product-ratingsContainer div.product-ratingsCount").inner_text().strip().replace("|", "").strip()
                    except Exception:
                        rating_count = "0"

                    try:
                        price_el = product.query_selector("span.product-discountedPrice") or product.query_selector("span.product-price") or product.query_selector("div.product-price span")
                        price_txt = price_el.inner_text().strip() if price_el else ""
                        if price_txt:
                            mpr = re.search(r'(₹|Rs\.?)[\s]*([\d,]+)', price_txt)
                            price = (mpr.group(1) + " " + mpr.group(2)) if mpr else price_txt
                        else:
                            price = ""
                    except Exception:
                        price = ""

                    link = ""
                    try:
                        buy_anchor = product.query_selector('a[href*="/buy"]')
                        if buy_anchor:
                            link = buy_anchor.get_attribute('href') or ""
                        else:
                            anchors = product.query_selector_all('a')
                            for a in anchors or []:
                                href = (a.get_attribute('href') or a.get_attribute('data-href') or a.get_attribute('data-url') or "").strip()
                                if not href:
                                    continue
                                if re.search(r'/\d{6,}', href):
                                    link = href
                                    break
                            if not link and anchors:
                                link = anchors[0].get_attribute('href') or ""

                        if link:
                            if not link.startswith("http"):
                                if not link.startswith("/"):
                                    link = "/" + link
                                link = "https://www.myntra.com" + link
                            if re.search(r'/\d{6,}(?:$|\?|/)', link) and not re.search(r'/buy(?:$|\?|/)', link):
                                link = re.sub(r'(\/\d{6,})(?:\/)?(?=$|\?|/)', r'\1/buy', link)
                    except Exception:
                        link = ""

                    long_name = ""
                    product_rating = rating
                    reviews_count = rating_count
                    last_12m_reviews_count = ""
                    ycounts = {2020: "", 2021: "", 2022: "", 2023: "", 2024: "", 2025: ""}

                    if link:
                        nonlocal_detail_page = False
                        try:
                            # Ensure the detail page is usable; recover if it was closed.
                            if detail_page.is_closed():
                                nonlocal_detail_page = True
                        except Exception:
                            nonlocal_detail_page = True
                        if nonlocal_detail_page:
                            try:
                                detail_page = _new_detail_page()
                            except Exception:
                                detail_page = _new_detail_page()

                        dp = detail_page
                        try:
                            nav_ok = False
                            for _nav_attempt in range(2):
                                try:
                                    dp.goto(link, timeout=45000, wait_until="domcontentloaded")
                                    dp.wait_for_selector("body", timeout=10000)
                                    nav_ok = True
                                    break
                                except Exception as e:
                                    msg = str(e) if e is not None else ""
                                    if "Target page" in msg or "closed" in msg.lower():
                                        try:
                                            detail_page = _new_detail_page()
                                            dp = detail_page
                                            continue
                                        except Exception:
                                            break
                                    raise

                            if not nav_ok:
                                raise RuntimeError("Failed to open product detail page")

                            # Auto-recover if Myntra shows the "Oops...Refresh" page.
                            if _is_myntra_oops_page(dp):
                                _recover_from_myntra_oops(dp, target_url=link, max_attempts=8)
                                try:
                                    dp.wait_for_selector("body", timeout=10000)
                                except Exception:
                                    pass
                            _wait_for_pdp_ready(dp, timeout_ms=25000)

                            try:
                                rt_el = (
                                    dp.query_selector("div.index-overallRating")
                                    or dp.query_selector("span.index-overallRating")
                                    or dp.query_selector("div.pdp-ratings span")
                                    or dp.query_selector("div.pdp-product-rating span")
                                )
                                rt_txt = rt_el.inner_text().strip() if rt_el else ""
                                if rt_txt:
                                    mrt = re.search(r"(\d+(?:\.\d+)?)", rt_txt)
                                    product_rating = mrt.group(1) if mrt else rt_txt
                            except Exception:
                                pass

                            try:
                                if not price:
                                    lp = dp.query_selector("span.pdp-price") or dp.query_selector("span.pdp-offers-price") or dp.query_selector("div.pdp-price span") or dp.query_selector("span[class*='pdp-price']")
                                    cp = lp.inner_text().strip() if lp else ""
                                    if cp:
                                        mpr = re.search(r'(₹|Rs\.?)[\s]*([\d,]+)', cp)
                                        price = (mpr.group(1) + " " + mpr.group(2)) if mpr else cp
                            except Exception:
                                pass

                            try:
                                rc_el = dp.query_selector("div.pdp-reviews-count") or dp.query_selector("div.product-ratingsCount") or dp.query_selector("span.pdp-reviews-count")
                                rc_txt = rc_el.inner_text().strip() if rc_el else ""
                                if rc_txt:
                                    mrc = re.search(r"(\d+[\,\d]*)", rc_txt)
                                    reviews_count = mrc.group(1) if mrc else rc_txt
                            except Exception:
                                pass

                            try:
                                ln_el = (
                                    dp.query_selector("h1.pdp-name")
                                    or dp.query_selector("h1.pdp-title")
                                    or dp.query_selector("div.pdp-title h1")
                                    or dp.query_selector("div.pdp-title")
                                )
                                long_name = (ln_el.inner_text().strip() if ln_el else "")
                                if not long_name:
                                    og = dp.query_selector("meta[property='og:title']")
                                    long_name = (og.get_attribute("content").strip() if og and og.get_attribute("content") else "")
                                if not long_name and brand and name:
                                    long_name = f"{brand} {name}".strip()
                            except Exception:
                                pass

                            try:
                                m_pid = None
                                m = re.search(r"/(\d{6,})(?:/buy)?(?:$|\?|/)", dp.url or "")
                                m_pid = m.group(1) if m else None
                                c12, yc = _get_review_counts(dp, product_id=m_pid)
                                last_12m_reviews_count = str(c12) if c12 != "" else ""
                                if isinstance(yc, dict):
                                    ycounts = yc
                            except Exception:
                                pass

                            try:
                                link = dp.url or link
                            except Exception:
                                pass

                            time.sleep(0.2 + random.uniform(0.05, 0.15))
                        except Exception:
                            pass

                    if not long_name:
                        long_name = f"{brand} {name}".strip() if (brand and name) else (name or "")

                    row = [
                        (brand or "").replace('\n', ' ').strip(),
                        (long_name or "").replace('\n', ' ').strip(),
                        (price or "").strip(),
                        (product_rating or "").strip(),
                        (reviews_count or "").strip(),
                        (link or "").strip(),
                        (str(last_12m_reviews_count) if last_12m_reviews_count is not None else ""),
                        (str(ycounts.get(2020, "")) if ycounts.get(2020, "") != "" else ""),
                        (str(ycounts.get(2021, "")) if ycounts.get(2021, "") != "" else ""),
                        (str(ycounts.get(2022, "")) if ycounts.get(2022, "") != "" else ""),
                        (str(ycounts.get(2023, "")) if ycounts.get(2023, "") != "" else ""),
                        (str(ycounts.get(2024, "")) if ycounts.get(2024, "") != "" else ""),
                        (str(ycounts.get(2025, "")) if ycounts.get(2025, "") != "" else ""),
                    ]

                    results.append(dict(zip(csv_headers, row)))

                    try:
                        with open(csv_name, 'a', newline='', encoding='utf-8') as f:
                            csv.writer(f).writerow(row)
                    except Exception as e:
                        print(f"Warning: could not write to CSV ({csv_name}): {e}")

                if max_pages and page_number >= max_pages:
                    print(f" Stopping after page {page_number} as requested (MYNTRA_MAX_PAGES={max_pages}).")
                    break

                try:
                    next_button = page.query_selector("li.pagination-next")
                except Exception as e:
                    print(f"Pagination unavailable or page closed: {e}")
                    break

                if next_button is None or "disabled" in (next_button.get_attribute("class") or ""):
                    print(" No more pages to scrape.")
                    break

                max_click_retries = 3
                click_attempt = 0
                while click_attempt < max_click_retries:
                    try:
                        first_product = page.query_selector("li.product-base")
                        first_product_html = first_product.inner_html() if first_product else ""
                        next_button.scroll_into_view_if_needed()
                        next_button.click()
                        page.wait_for_load_state("domcontentloaded")

                        page.wait_for_function(
                            "firstHTML => document.querySelector('li.product-base') && document.querySelector('li.product-base').innerHTML !== firstHTML",
                            arg=first_product_html,
                            timeout=20000,
                        )
                        page_number += 1
                        time.sleep(0.4 + random.uniform(0.1, 0.3))
                        break
                    except Exception as e:
                        click_attempt += 1
                        print(f"Attempt {click_attempt} to navigate to next page failed: {e}")
                        time.sleep(0.4)
                else:
                    print(" Failed to navigate to next page after retries. Stopping.")
                    break

        try:
            browser.close()
        except Exception:
            pass

    wb = Workbook()
    ws = wb.active
    ws.append(csv_headers)
    for item in results:
        ws.append([item.get(h, "") for h in csv_headers])

    try:
        wb.save(xlsx_name)
    except Exception:
        ts = time.strftime("%Y%m%d_%H%M%S")
        wb.save(f"myntra_h_and_m_bodysuit_{ts}.xlsx")

    print(f"\n Scraping completed. {len(results)} products saved to {xlsx_name}.")


if __name__ == "__main__":
    scrape_mns_myntra()
