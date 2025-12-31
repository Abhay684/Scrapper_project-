import asyncio
import random
import re
import urllib.parse
from playwright.async_api import async_playwright
from openpyxl import Workbook
from tqdm.asyncio import tqdm_asyncio

# ================= COOKIES ================= #

async def accept_cookies(page):
    try:
        await page.wait_for_selector("button:has-text('Accept')", timeout=5000)
        await page.click("button:has-text('Accept')")
        await asyncio.sleep(1)
    except:
        pass

# ================= OVERLAY HANDLING ================= #

async def close_signup_popup(page):
    try:
        # Attentive / signup modal close button
        btn = await page.query_selector("button[data-testid='closeIcon'], #closeIconContainer")
        if btn:
            await btn.click()
            await asyncio.sleep(0.5)
    except:
        pass

# ================= CONFIG ================= #

COLLECTION_URLS = [
    "https://spanx.com/collections/intimates",
    "https://spanx.com/collections/best-sellers",
    "https://spanx.com/collections/new-arrivals",
    "https://spanx.com/collections/clothing",
    "https://spanx.com/collections/airessentials",
    "https://spanx.com/collections/jeans","https://spanx.com/collections/shapewear",
    "https://spanx.com/collections/matching-sets",
    "https://spanx.com/collections/active",
]

MAX_CONTEXTS = 3
MAX_CONCURRENT_PDP = 6
OUTPUT_FILE = "spanx_products.xlsx"

excel_lock = asyncio.Lock()

# ================= UTIL ================= #

async def human_sleep(a=0.6, b=1.3):
    await asyncio.sleep(random.uniform(a, b))

def months_ago(text):
    if not text:
        return None
    text = text.lower()
    m = re.search(r"(\d+)\s*(day|week|month|year)s?\s*ago", text)
    if not m:
        return None
    n, u = int(m.group(1)), m.group(2)
    if u == "day":
        return n / 30
    if u == "week":
        return n / 4
    if u == "month":
        return n
    if u == "year":
        return n * 12
    return None

# ================= COLLECTION SCROLL ================= #

async def collect_product_links(page):
    links = set()
    stagnant = 0
    prev = 0

    await page.wait_for_selector("a[href*='/products/']", timeout=30000)

    for _ in range(60):
        # 🔴 close signup popup if it appears
        await close_signup_popup(page)

        # 🧠 human-like scroll burst
        await page.mouse.wheel(0, random.randint(400, 700))
        await asyncio.sleep(random.uniform(0.3, 0.6))

        await page.evaluate(
            "() => window.scrollBy(0, Math.floor(window.innerHeight * 0.6))"
        )
        await asyncio.sleep(random.uniform(0.4, 0.8))

        anchors = await page.query_selector_all("a[href*='/products/']")
        for a in anchors:
            href = await a.get_attribute("href")
            if href:
                links.add(urllib.parse.urljoin("https://spanx.com", href))

        if len(links) == prev:
            stagnant += 1
        else:
            stagnant = 0

        if stagnant >= 6:
            # ⏳ wait a bit more in case lazy load is slow
            await asyncio.sleep(random.uniform(2.0, 3.0))
            await close_signup_popup(page)

            anchors_check = await page.query_selector_all("a[href*='/products/']")
            if len(anchors_check) == prev:
                break
            else:
                stagnant = 0

        prev = len(links)

    return list(links)

# ================= REVIEW EXPAND ================= #

async def expand_spanx_reviews(page):
    for _ in range(25):
        try:
            btn = await page.query_selector("button:has-text('Load more')")
            if not btn:
                break
            await btn.scroll_into_view_if_needed()
            await asyncio.sleep(0.6)
            await btn.click()
            await asyncio.sleep(1.2)
        except:
            break

# ================= PDP ================= #

async def scrape_pdp(semaphore, context, url, ws_products, ws_reviews, wb):
    async with semaphore:
        page = await context.new_page()
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_selector("h1", timeout=30000)
            await human_sleep(1.5, 2.5)

            title = await page.inner_text("h1")

            price = ""
            if await page.query_selector("[data-testid='regular-price-value'] span"):
                price = await page.inner_text("[data-testid='regular-price-value'] span")

            rating = "0"
            if await page.query_selector("#reviews .font-sans.text-lg"):
                rating = await page.inner_text("#reviews .font-sans.text-lg")

            total_reviews = "0"
            if await page.query_selector("#reviews small"):
                txt = await page.inner_text("#reviews small")
                nums = re.findall(r"\d+", txt)
                total_reviews = nums[0] if nums else "0"

            # ---- REVIEWS ---- #
            await page.evaluate("""
                () => document.querySelector('section')?.scrollIntoView({behavior:'smooth'})
            """)
            await asyncio.sleep(2)

            await expand_spanx_reviews(page)

            cards = await page.query_selector_all(
                "section div.flex.w-full.gap-8"
            )

            recent_count = 0

            for card in cards:
                try:
                    age_el = await card.query_selector(
                        "small:text-matches('(day|week|month|year)s? ago', 'i')"
                    )
                    if not age_el:
                        continue

                    age_text = (await age_el.inner_text()).strip()
                    m = months_ago(age_text)

                    if m is not None and m < 12:
                        recent_count += 1

                        review_text = ""
                        try:
                            review_text = await card.inner_text(
                                "p.OneLinkNoTx"
                            )
                        except:
                            pass

                        ws_reviews.append([
                            title, url, age_text, review_text.strip()
                        ])
                except:
                    continue

            async with excel_lock:
                ws_products.append([
                    title, price, rating, total_reviews, recent_count, url
                ])
                wb.save(OUTPUT_FILE)

            print(f"✅ {title} | <12M Reviews: {recent_count}")

        except Exception as e:
            print(f"⚠️ PDP failed: {url} → {e}")

        finally:
            await page.close()

# ================= MAIN ================= #

async def main():
    wb = Workbook()
    ws_reviews = wb.active
    ws_reviews.title = "Reviews"
    ws_reviews.append(["Product", "URL", "Age", "Review"])

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir="spanx_user_data",
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_PDP)

        for i, col in enumerate(COLLECTION_URLS):
            sheet = col.split("/collections/")[-1][:31]
            ws_products = wb.create_sheet(sheet)
            ws_products.append([
                "Name", "Price", "Rating",
                "Total Reviews", "<12M Reviews", "URL"
            ])

            ctx = context
            page = await ctx.new_page()
            await page.goto(col, timeout=60000)
            await accept_cookies(page)

            links = await collect_product_links(page)
            await page.close()

            tasks = [
                scrape_pdp(semaphore, ctx, link, ws_products, ws_reviews, wb)
                for link in links
            ]

            await tqdm_asyncio.gather(*tasks)

        await context.close()

    wb.save(OUTPUT_FILE)
    print("🎉 SPANX SCRAPING COMPLETE")

if __name__ == "__main__":
    asyncio.run(main())