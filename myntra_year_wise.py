from playwright.sync_api import sync_playwright
import time, random
from openpyxl import Workbook
import re
import csv
import os
from datetime import datetime, timedelta
import json





def scrape_mns_myntra():
    url = "https://www.myntra.com/jockey-bra?f=Color%3ABlack_36454f&rawQuery=jockey%20bra%20"
    results = []
    csv_name = "myntra_h_and_m_bodysuit.csv"
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
    if (not os.path.exists(csv_name)) or (os.path.exists(csv_name) and os.path.getsize(csv_name) == 0):
        try:
            with open(csv_name, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(csv_headers)
        except Exception as e:
            print(f"Could not create CSV file {csv_name}: {e}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768}
        )
        def should_block(req):
            rt = req.resource_type
            url = req.url
            if rt in ["image", "font", "media", "stylesheet"]:
                return True
            if re.search(r"(doubleclick|google-analytics|adservice|facebook|hotjar|segment|optimizely|ads|tracking)", url, re.I):
                return True
            return False
        context.route("**/*", lambda route: route.abort() if should_block(route.request) else route.continue_())
        page = context.new_page()
        detail_page = context.new_page()
        page.set_default_timeout(20000)
        detail_page.set_default_timeout(15000)

        def _parse_review_date(date_text: str):
            if not date_text:
                return None
            s = re.sub(r"\s+", " ", date_text).strip()
            s = s.replace(",", " ")

            # Python's strptime doesn't accept "Sept" for %b; Myntra often uses it.
            # Normalize common variants.
            s = re.sub(r"\bSept\b", "Sep", s, flags=re.I)

            now = datetime.now()
            # Myntra sometimes renders: "Keerthi M S6 Oct 2024" (no space before day)
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

            # e.g. "10 Oct 2024" / "10 October 2024" / "10 Oct, 24"
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

            # e.g. "10 Oct" / "10 October" (assume current year)
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
                        # If parsing with current year produces a future date (unlikely), fall back 1 year.
                        if dt > datetime.now() + timedelta(days=2):
                            dt = datetime(dt.year - 1, dt.month, dt.day)
                        return dt
                    except Exception:
                        pass

            # e.g. "Oct 2024" (assume day=1)
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

            # e.g. "10/10/2024" or "10-10-2024" (assume d/m/y)
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

        def _try_click_view_all_reviews(dp):
            candidates = [
                "a.detailed-reviews-allReviews",
                "a[href^='/reviews/']",
                "a[href*='/reviews/']",
                "a:has-text('View all reviews')",
                "button:has-text('View all reviews')",
                r"text=/View\s+all\s+reviews/i",
                "a:has-text('See all reviews')",
                "button:has-text('See all reviews')",
                r"text=/See\s+all\s+reviews/i",
                r"text=/All\s+reviews/i",
            ]
            for sel in candidates:
                try:
                    loc = dp.locator(sel).first
                    if loc and loc.count() > 0:
                        loc.scroll_into_view_if_needed(timeout=2000)
                        loc.click(timeout=3000)
                        try:
                            dp.wait_for_url("**/reviews/**", timeout=8000)
                        except Exception:
                            pass
                        try:
                            dp.wait_for_load_state("domcontentloaded", timeout=15000)
                        except Exception:
                            pass
                        try:
                            dp.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass
                        time.sleep(0.3)
                        return True
                except Exception:
                    continue
            return False

        def _scroll_to_reviews_area(dp, max_scrolls=10):
            # Myntra often lazy-loads the reviews section far below the fold.
            for _ in range(max_scrolls):
                try:
                    # If the button exists, we're already in the right area.
                    if dp.locator("text=/View\\s+all\\s+reviews/i").count() > 0:
                        return True
                    if dp.locator("text=/See\\s+all\\s+reviews/i").count() > 0:
                        return True
                    if dp.locator("text=/Ratings|Reviews/i").count() > 0:
                        try:
                            dp.locator("text=/Ratings|Reviews/i").first.scroll_into_view_if_needed(timeout=1500)
                            time.sleep(0.2)
                        except Exception:
                            pass
                        return True
                except Exception:
                    pass
                try:
                    dp.evaluate("() => window.scrollBy(0, Math.max(900, window.innerHeight * 0.9))")
                except Exception:
                    pass
                time.sleep(0.25)
            return False

        def _scroll_reviews(dp):
            # Incremental scrolling works better than jumping to bottom for lazy-loaded reviews.
            # Try scroll-container first, then page scroll.
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
                          // Prefer containers that look like they hold reviews.
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
            # Returns list of dicts: {key, dateText}
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
                                                // Pick the last date-like token from the card text.
                                                // Important: avoid treating plain numbers in other phrases as dates.
                                                // We only match date-like tokens that contain a month name.
                                                const r1y4 = raw.match(/(\d{1,2}\s*[A-Za-z]{3,9}\s*,?\s*\d{4})\b/g);
                                                if (r1y4 && r1y4.length) return r1y4[r1y4.length - 1];

                                                const r1y2 = raw.match(/(\d{1,2}\s*[A-Za-z]{3,9}\s*,?\s*\d{2})\b/g);
                                                if (r1y2 && r1y2.length) return r1y2[r1y2.length - 1];

                                                const rmy = raw.match(/([A-Za-z]{3,9}\s*\d{4})\b/g);
                                                if (rmy && rmy.length) return rmy[rmy.length - 1];

                                                // Dates sometimes omit the year (assume current year in Python parser).
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
                                                // force unsigned
                                                return (h >>> 0).toString(16);
                                            };

                                            const pickReviewId = (el) => {
                                                if (!el || !el.getAttribute) return "";
                                                const attrs = [
                                                    'data-reviewid','data-review-id','data-reviewId',
                                                    'data-id','data-uuid','data-review',
                                                    'id'
                                                ];
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
                                                // Do not store comment text; use it only for stable de-duping.
                                                // Hash avoids collisions caused by truncation (e.g. many short reviews on same date).
                                                return d + '|h:' + djb2(t);
                                            };

                                            // Primary strategy: find date elements and build review blocks around them.
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

                                            const pickDateText = (node) => {
                                                const dateCandidates2 = node.querySelectorAll('time, [class*=date i], [data-testid*=date i]');
                                                for (const d of dateCandidates2) {
                                                    const dtAttr = (d.getAttribute && (d.getAttribute('datetime') || d.getAttribute('dateTime'))) || '';
                                                    if (dtAttr && dtAttr.trim()) return dtAttr.trim();
                                                    const t = (d.textContent || '').trim();
                          if (t && t.length <= 40) return t;
                        }
                                                const raw = normalize(node.textContent);
                                                return pickDateFromText(raw);
                      };
                      for (const sel of reviewSelectors) {
                                                const nodes = Array.from(document.querySelectorAll(sel)).slice(0, 2000);
                        for (const node of nodes) {
                                                    const raw0 = normalize(node.textContent);
                                                    if (!raw0 || raw0.length < 10) continue;
                                                    // Must contain a date-like token to be treated as a review card.
                                                    const dateHint = pickDateFromText(raw0);
                                                    if (!dateHint) continue;

                                                    // If this node is a small nested element, climb to a larger container that
                                                    // still contains the same date token (so we capture the comment text).
                                                    let container = node;
                                                                                                        try {
                                                                                                            if (container && container.closest) {
                                                                                                                const cc = container.closest(reviewSel);
                                                                                                                if (cc) container = cc;
                                                                                                            }
                                                                                                        } catch(e) {}
                                                    const dhCompact = (dateHint || '').replace(/\s+/g, '').toLowerCase();
                                                    for (let i = 0; i < 6; i++) {
                                                        const p = container.parentElement;
                                                        if (!p) break;
                                                        const pt = (p.textContent || '').replace(/\s+/g,' ').trim();
                                                        const ct = (container.textContent || '').replace(/\s+/g,' ').trim();
                                                        if (!pt || pt.length < ct.length + 25) break;
                                                        if (pt.length > 1600) break;
                                                        const ptCompact = pt.replace(/\s+/g, '').toLowerCase();
                                                        if (dhCompact && ptCompact.indexOf(dhCompact) === -1) break;
                                                        container = p;
                                                    }

                                                                                                        const dt = normalize(pickDateText(node) || dateHint);
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

        def _ensure_reviews_sorted(dp, mode: str):
            # Supported: "helpful" (Most Helpful) and "recent" (Most recent).
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

                if mode == "recent":
                    targets = [
                        "text=Most recent",
                        "text=Most Recent",
                        r"text=/Most\s+recent/i",
                        r"text=/Newest/i",
                    ]
                else:
                    targets = [
                        "text=Most helpful",
                        "text=Most Helpful",
                        r"text=/Most\s+helpful/i",
                        r"text=/Helpful/i",
                    ]

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

        def _extract_total_reviews_shown(dp):
            # Best-effort: extract the total review count shown on the page (e.g., "View all 15 reviews").
            try:
                txt = ""
                try:
                    a = dp.locator("a.detailed-reviews-allReviews").first
                    if a and a.count() > 0:
                        txt = (a.inner_text(timeout=1000) or "").strip()
                except Exception:
                    txt = ""

                if not txt:
                    # Fallback: look for a compact "XX reviews" snippet near the top of the page.
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
            # Best-effort: extract the "View all reviews" link without navigating away.
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

        def _get_review_counts(dp, product_id=None, max_scrolls=180, max_idle=8):
            # Returns:
            # - rolling_last_12m_count: int or "" if not reliably parseable
            # - year_counts: dict[int, int] for years 2020..2025 (or "" values if not parseable)
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
                # User explicitly wants "Most Helpful" sorting.
                sort_mode = "helpful"
                _ensure_reviews_sorted(rp, sort_mode)
            except Exception:
                try:
                    if rp:
                        rp.close()
                except Exception:
                    pass
                return "", {y: "" for y in years}

            seen_total = 0
            idle = 0
            oldest_parsed = None
            last_oldest_parsed = None
            start_t = time.monotonic()
            max_seconds = 120

            # For "Most Helpful" sorting, results are NOT chronological.
            # So we can't early-stop based on date; instead load until we reach total shown.
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
                        # Some pages show a "Load more" button in addition to infinite scroll.
                        # Click it a few times per loop (it can re-render).
                        for _clicks in range(3):
                            loc = rp.locator(load_sel).first
                            if not loc:
                                break
                            try:
                                if loc.count() <= 0:
                                    break
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
                prev_total = seen_total
                if total <= prev_total:
                    idle += 1
                else:
                    idle = 0
                    seen_total = total

                parsed_dates = []
                for r in reviews:
                    dt = _parse_review_date(r.get("dateText", ""))
                    if dt:
                        parsed_dates.append(dt)

                if parsed_dates:
                    oldest_parsed = min(parsed_dates) if oldest_parsed is None else min(oldest_parsed, min(parsed_dates))
                # If we know the total count shown on the page, stop once we've loaded that many.
                if isinstance(shown_total, int) and shown_total > 0:
                    if seen_total >= shown_total and idle >= 1:
                        break

                # If the oldest parsed date is no longer changing AND total isn't increasing,
                # treat that as additional idle signal.
                if last_oldest_parsed is not None and oldest_parsed is not None:
                    if oldest_parsed == last_oldest_parsed and total <= prev_total:
                        idle += 1
                last_oldest_parsed = oldest_parsed

                if idle >= max_idle:
                    break

                _scroll_reviews(rp)
                # Give the site time to fetch/render newly loaded review cards.
                try:
                    rp.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                rp.wait_for_timeout(int(450 + random.uniform(75, 175)))

            # Count from the finally loaded DOM on the reviews page.
            rolling_last_12m_count = 0
            year_counts = {y: 0 for y in years}
            any_reviews = []
            try:
                any_reviews = _extract_reviews_with_dates(rp)
                for r in any_reviews:
                    dt = _parse_review_date(r.get("dateText", ""))
                    if not dt:
                        continue
                    if window_start <= dt <= window_end:
                        rolling_last_12m_count += 1
                    if dt.year in year_counts:
                        year_counts[dt.year] += 1
            except Exception:
                rolling_last_12m_count = 0
                year_counts = {y: 0 for y in years}

            # Optional debug: dump detected date strings and parsed results (no review text).
            try:
                if os.environ.get("MYNTRA_DEBUG_REVIEW_DATES", "").strip() == "1" and product_id:
                    os.makedirs(os.path.join(os.getcwd(), "debug_reviews"), exist_ok=True)
                    out_path = os.path.join(os.getcwd(), "debug_reviews", f"review_dates_{product_id}.csv")
                    with open(out_path, "w", newline="", encoding="utf-8") as f:
                        w = csv.writer(f)
                        w.writerow(["dateText", "parsed", "in_range"])
                        for r in any_reviews:
                            dt_raw = (r.get("dateText", "") or "").strip()
                            dt = _parse_review_date(dt_raw)
                            parsed = dt.isoformat(sep=" ") if dt else ""
                            in_range = "1" if (dt and (year_start <= dt <= window_end)) else "0"
                            w.writerow([dt_raw, parsed, in_range])
            except Exception:
                pass

            # Sanity cap: a subset (2025-only) cannot exceed the total shown reviews.
            try:
                shown_total = _extract_total_reviews_shown(rp)
                if isinstance(shown_total, int) and shown_total >= 0:
                    if rolling_last_12m_count > shown_total:
                        rolling_last_12m_count = shown_total
            except Exception:
                pass

            try:
                rp.close()
            except Exception:
                pass

            # Avoid misleading 0 when reviews exist but dates are not parseable.
            if rolling_last_12m_count == 0 and any_reviews:
                # If we couldn't parse *any* dates, return blanks instead of 0s.
                # If some dates were parseable, this block won't trigger.
                parsed_any = False
                for r in any_reviews:
                    if _parse_review_date(r.get("dateText", "")):
                        parsed_any = True
                        break
                if not parsed_any:
                    return "", {y: "" for y in years}

            return rolling_last_12m_count, year_counts

        def load_page_with_retries(url, retries=3, delay=2):
            for attempt in range(retries):
                try:
                    page.goto(url, timeout=90000, wait_until="domcontentloaded")
                    try:
                        page.wait_for_load_state("networkidle", timeout=45000)
                    except Exception:
                        pass

                    # Myntra listing markup can vary; wait for product cards.
                    try:
                        page.wait_for_function("() => document.querySelectorAll('li.product-base').length > 0", timeout=60000)
                    except Exception:
                        # Fallback selectors
                        try:
                            page.wait_for_selector("li.product-base", timeout=60000)
                        except Exception:
                            page.wait_for_selector("ul.results-base", timeout=60000)
                    time.sleep(0.5 + random.uniform(0.0, 0.3))
                    return True
                except Exception as e:
                    print(f"Load attempt {attempt + 1} failed: {e}")
                    time.sleep(delay)
            print("All load attempts failed, pausing for 30 seconds before returning.")
            time.sleep(30)
            return False

        if not load_page_with_retries(url):
            print("Failed to load the initial page after retries. Exiting.")
            browser.close()
            return

        page_number = 1
        try:
            max_pages_env = os.environ.get("MYNTRA_MAX_PAGES", "")
            max_pages = int(max_pages_env) if max_pages_env.strip() else 0
            if max_pages < 0:
                max_pages = 0
        except Exception:
            max_pages = 0
        while True:
            print(f"📄 Scraping page {page_number}...")
            products = page.query_selector_all("li.product-base")
            # Optional: limit products for quick debugging/testing.
            try:
                max_products = int(os.environ.get("MYNTRA_MAX_PRODUCTS", "0") or "0")
            except Exception:
                max_products = 0
            if max_products and max_products > 0:
                products = (products or [])[:max_products]
            for product in products:
                try:
                    name = product.query_selector("h4.product-product").inner_text().strip()
                except:
                    name = ""

                try:
                    brand = product.query_selector("h3.product-brand").inner_text().strip()
                except:
                    brand = ""

                try:
                    rating = product.query_selector("div.product-ratingsContainer span").inner_text().strip()
                except:
                    rating = "0"

                try:
                    rating_count = product.query_selector("div.product-ratingsContainer div.product-ratingsCount").inner_text().strip().replace("|", "").strip()
                except:
                    rating_count = "0"
                try:
                    price_el = product.query_selector("span.product-discountedPrice") or product.query_selector("span.product-price") or product.query_selector("div.product-price span")
                    price_txt = price_el.inner_text().strip() if price_el else ""
                    if price_txt:
                        mpr = re.search(r'(₹|Rs\.?)[\s]*([\d,]+)', price_txt)
                        price = (mpr.group(1) + " " + mpr.group(2)) if mpr else price_txt
                except:
                    price = ""



                try:
                    link = ""
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
                        link = link.strip()
                        if not link.startswith("http"):
                            if not link.startswith("/"):
                                link = "/" + link
                            link = "https://www.myntra.com" + link
                        if re.search(r'/\d{6,}(?:$|\?|/)', link) and not re.search(r'/buy(?:$|\?|/)', link):
                            link = re.sub(r'(\/\d{6,})(?:\/)?(?=$|\?|/)', r'\1/buy', link)
                except Exception:
                    link = ""

                short_name = name
                long_name = ""
                price = ""
                reviews_count = ""
                reviews_text = ""
                product_rating = rating
                last_12m_reviews_count = ""
                y2020 = ""
                y2021 = ""
                y2022 = ""
                y2023 = ""
                y2024 = ""
                y2025 = ""


                try:
                    img = product.query_selector('img')
                    if img:
                        srcset = img.get_attribute('srcset') or img.get_attribute('data-srcset') or ""
                        src = img.get_attribute('src') or img.get_attribute('data-src') or img.get_attribute('data-original') or ""
                        if srcset:
                            parts = [p.strip().split(' ')[0] for p in srcset.split(',') if p.strip()]
                            if parts:
                                src = parts[-1]
                        if not src:
                            sources = product.query_selector_all('picture source')
                            for so in sources or []:
                                ss = so.get_attribute('srcset') or so.get_attribute('data-srcset') or ""
                                if ss:
                                    parts = [p.strip().split(' ')[0] for p in ss.split(',') if p.strip()]
                                    if parts:
                                        src = parts[-1]
                                        break
                        if not src:
                            style_el = product.query_selector('[style*="background-image"]')
                            st = style_el.get_attribute('style') if style_el else ""
                            if st:
                                m = re.search(r'url\(([^)]+)\)', st)
                                if m:
                                    src = m.group(1).strip().strip('"\'')
                        if src and src.startswith('//'):
                            src = 'https:' + src

                except:
                    pass

                reviews_count = rating_count

                if link:
                    dp = detail_page
                    try:
                        dp.goto(link, timeout=25000, wait_until="domcontentloaded")
                        dp.wait_for_selector("body", timeout=10000)
                        try:
                            rt_el = (
                                dp.query_selector("div.index-overallRating")
                                or dp.query_selector("span.index-overallRating")
                                or dp.query_selector("div.pdp-ratings span")
                                or dp.query_selector("div.pdp-product-rating span")
                            )
                            rt_txt = rt_el.inner_text().strip() if rt_el else ""
                            if not rt_txt:
                                rt_txt = dp.evaluate("() => { const el = document.querySelector('[class*=overall][class*=Rating]'); return el ? el.textContent.trim() : '' }")
                            if rt_txt:
                                mrt = re.search(r"(\d+(?:\.\d+)?)", rt_txt)
                                product_rating = mrt.group(1) if mrt else rt_txt
                        except:
                            pass
                        try:
                            if not price:
                                lp = dp.query_selector("span.pdp-price") or dp.query_selector("span.pdp-offers-price") or dp.query_selector("div.pdp-price span") or dp.query_selector("span[class*='pdp-price']")
                                cp = lp.inner_text().strip() if lp else ""
                                if cp:
                                    mpr = re.search(r'(₹|Rs\.?)[\s]*([\d,]+)', cp)
                                    price = (mpr.group(1) + " " + mpr.group(2)) if mpr else cp
                        except:
                            pass

                        try:
                            rc_el = dp.query_selector("div.pdp-reviews-count") or dp.query_selector("div.product-ratingsCount") or dp.query_selector("span.pdp-reviews-count")
                            rc_txt = rc_el.inner_text().strip() if rc_el else ""
                            if rc_txt:
                                mrc = re.search(r"(\d+[\,\d]*)", rc_txt)
                                reviews_count = mrc.group(1) if mrc else rc_txt
                        except:
                            pass
                        try:
                            reviews_elems = dp.query_selector_all("div[itemprop='review'], div.pdp-review, div.review, .reviewCard, .user-review, li.review")
                            texts = []
                            for r in reviews_elems or []:
                                t = r.inner_text().strip()
                                if t:
                                    t2 = re.sub(r'\s+', ' ', t)
                                    texts.append(t2)
                                if len(texts) >= 3:
                                    break
                            if texts:
                                reviews_text = " | ".join(texts)
                        except:
                            pass

                        # Last 12 months reviews count + details
                        try:
                            m_pid = None
                            try:
                                m = re.search(r"/(\d{6,})(?:/buy)?(?:$|\?|/)", dp.url or "")
                                m_pid = m.group(1) if m else None
                            except Exception:
                                m_pid = None

                            c12, ycounts = _get_review_counts(dp, product_id=m_pid)
                            last_12m_reviews_count = str(c12) if c12 != "" else ""
                            if isinstance(ycounts, dict):
                                y2020 = str(ycounts.get(2020, "")) if ycounts.get(2020, "") != "" else ""
                                y2021 = str(ycounts.get(2021, "")) if ycounts.get(2021, "") != "" else ""
                                y2022 = str(ycounts.get(2022, "")) if ycounts.get(2022, "") != "" else ""
                                y2023 = str(ycounts.get(2023, "")) if ycounts.get(2023, "") != "" else ""
                                y2024 = str(ycounts.get(2024, "")) if ycounts.get(2024, "") != "" else ""
                                y2025 = str(ycounts.get(2025, "")) if ycounts.get(2025, "") != "" else ""
                        except Exception:
                            last_12m_reviews_count = ""
                            y2020 = ""
                            y2021 = ""
                            y2022 = ""
                            y2023 = ""
                            y2024 = ""
                            y2025 = ""
                        try:
                            try:
                                dp.wait_for_selector("h1.pdp-name, h1.pdp-title, div.pdp-title h1", timeout=15000)
                            except Exception:
                                pass
                            ln_el = (
                                dp.query_selector("h1.pdp-name")
                                or dp.query_selector("h1.pdp-title")
                                or dp.query_selector("div.pdp-title h1")
                                or dp.query_selector("div.pdp-title")
                            )
                            ln = ln_el.inner_text().strip() if ln_el else ""
                            if not ln:
                                og = dp.query_selector("meta[property='og:title']")
                                ln = (og.get_attribute("content").strip() if og and og.get_attribute("content") else "")
                            if not ln:
                                title_el = dp.query_selector("title")
                                ln = title_el.inner_text().strip() if title_el else ""
                            if not ln and brand and short_name:
                                ln = f"{brand} {short_name}".strip()
                            long_name = ln
                        except:
                            long_name = ""
                        try:
                            if not brand:
                                b_meta = dp.query_selector("meta[itemprop='brand']") or dp.query_selector("meta[property='product:brand']")
                                b_txt = b_meta.get_attribute("content").strip() if b_meta and b_meta.get_attribute("content") else ""
                                if not b_txt:
                                    b_el = dp.query_selector("div.pdp-title a") or dp.query_selector("a.pdp-url") or dp.query_selector("a.pdp-brand")
                                    b_txt = b_el.inner_text().strip() if b_el else ""
                                if b_txt:
                                    brand = b_txt
                        except:
                            pass

                        try:
                            if False:
                                cand = dp.evaluate("(sel) => { const el = document.querySelector(sel); if(!el) return ''; const t = el.textContent; try { let d = JSON.parse(t); if(Array.isArray(d)) d = d[0]; let img = d && d.image; if(Array.isArray(img)) img = img[0]; return img || ''; } catch(e){ return '' } }", 'script[type="application/ld+json"]')

                            if False:
                                cand2 = dp.evaluate("() => { const scripts = [...document.querySelectorAll('script[type=\"application/ld+json\"]')]; for (const el of scripts){ try { let d = JSON.parse(el.textContent); const pick = (obj) => { let img = obj && obj.image; if(Array.isArray(img)) img = img[0]; return (typeof img==='string') ? img : ''; }; if(Array.isArray(d)) { for (const o of d){ const r = pick(o); if(r) return r; } } else { const r = pick(d); if(r) return r; } } catch(e){} } return '' }")

                            if False:
                                preloads = dp.query_selector_all('link[rel="preload"][as="image"]')
                                for pl in preloads or []:
                                    href = pl.get_attribute('href') or ''
                            if False:
                                sel_imgs = dp.query_selector_all('div.pdp-image img, div.image-grid img, div.image-viewer img, img')
                                for im in sel_imgs or []:
                                    s = im.get_attribute('src') or im.get_attribute('data-src') or im.get_attribute('data-original') or ""
                                    if s and s.startswith('//'):
                                        s = 'https:' + s
                            if False:
                                style_el = dp.query_selector('[style*="background-image"]')
                                st = style_el.get_attribute('style') if style_el else ""
                                if st:
                                    m = re.search(r'url\(([^)]+)\)', st)
                                    if m:
                                        s = m.group(1).strip().strip('"\'')
                                        if s and s.startswith('//'):
                                            s = 'https:' + s

                        except:
                            pass
                        try:
                            link = dp.url or link
                        except:
                            pass
                        time.sleep(0.2 + random.uniform(0.05, 0.15))
                    except Exception as e:
                        pass





                if not long_name:
                    if brand and short_name:
                        long_name = f"{brand} {short_name}".strip()
                    else:
                        long_name = short_name

                results.append({
                    "Brand Name": brand,
                    "Full Name": long_name,
                    "Price": price,
                    "Product Rating": product_rating,
                    "Customer Reviews Count": reviews_count,
                    "Product URL": link,
                    "Customer Reviews (Last 12 Months) Count": last_12m_reviews_count,
                    "Customer Reviews 2020 Count": y2020,
                    "Customer Reviews 2021 Count": y2021,
                    "Customer Reviews 2022 Count": y2022,
                    "Customer Reviews 2023 Count": y2023,
                    "Customer Reviews 2024 Count": y2024,
                    "Customer Reviews 2025 Count": y2025,
                })
                # Save progress to CSV immediately
                try:
                    row = [
                        (brand or "").replace('\n', ' ').strip(),
                        (long_name or "").replace('\n', ' ').strip(),
                        (price or "").strip(),
                        (product_rating or "").strip(),
                        (reviews_count or "").strip(),
                        (link or "").strip(),
                        (last_12m_reviews_count or "").strip(),
                        (y2020 or "").strip(),
                        (y2021 or "").strip(),
                        (y2022 or "").strip(),
                        (y2023 or "").strip(),
                        (y2024 or "").strip(),
                        (y2025 or "").strip(),
                    ]
                    with open(csv_name, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(row)
                except Exception as e:
                    print(f"Warning: could not write to CSV ({csv_name}): {e}")

            if max_pages and page_number >= max_pages:
                print(f"✅ Stopping after page {page_number} as requested (MYNTRA_MAX_PAGES={max_pages}).")
                break
            try:
                next_button = page.query_selector("li.pagination-next")
            except Exception as e:
                print(f"Pagination unavailable or page closed: {e}")
                break
            if next_button is None or "disabled" in (next_button.get_attribute("class") or ""):
                print("✅ No more pages to scrape.")
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
                        "firstHTML => document.querySelector('li.product-base') && "
                        "document.querySelector('li.product-base').innerHTML !== firstHTML",
                        arg=first_product_html,
                        timeout=20000
                    )
                    page_number += 1
                    time.sleep(0.4 + random.uniform(0.1, 0.3))
                    break
                except Exception as e:
                    click_attempt += 1
                    print(f"Attempt {click_attempt} to navigate to next page failed: {e}")
                    time.sleep(0.4)
            else:
                print("❌ Failed to navigate to next page after retries. Stopping.")
                break

        browser.close()

    wb = Workbook()
    ws = wb.active
    headers = [
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
    ws.append(headers)
    for item in results:
        ws.append([
            item["Brand Name"],
            item["Full Name"],
            item.get("Price", ""),
            item["Product Rating"],
            item["Customer Reviews Count"],
            item["Product URL"],
            item.get("Customer Reviews (Last 12 Months) Count", ""),
            item.get("Customer Reviews 2020 Count", ""),
            item.get("Customer Reviews 2021 Count", ""),
            item.get("Customer Reviews 2022 Count", ""),
            item.get("Customer Reviews 2023 Count", ""),
            item.get("Customer Reviews 2024 Count", ""),
            item.get("Customer Reviews 2025 Count", ""),
        ])
    saved_name = "myntra_h_and_m_bodysuit.xlsx"
    try:
        wb.save(saved_name)
    except Exception:
        ts = time.strftime("%Y%m%d_%H%M%S")
        saved_name = f"myntra_h_and_m_bodysuit_{ts}.xlsx"
        try:
            wb.save(saved_name)
        except Exception:
            saved_name = "myntra_h_and_m_bodysuit.xlsx"
            wb.save(saved_name)

    print(f"\n🎉 Scraping completed. {len(results)} products saved to {saved_name}.")


if __name__ == "__main__":
    scrape_mns_myntra()

