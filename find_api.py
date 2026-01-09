from playwright.sync_api import sync_playwright
import json

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        api_requests = []

        def handle_request(request):
            if "review" in request.url.lower() or "v1" in request.url.lower() or "v2" in request.url.lower():
                if request.resource_type in ["fetch", "xhr"]:
                    api_requests.append({
                        "url": request.url,
                        "method": request.method,
                        "headers": request.headers
                    })

        page.on("request", handle_request)

        # Visit a product page. I'll use a known product ID if possible.
        # Let's try to find a product from the listing page first.
        listing_url = "https://www.myntra.com/jockey-solid-hipster-panties"
        print(f"Visiting listing page: {listing_url}")
        page.goto(listing_url, wait_until="networkidle")
        
        # Get the first product link
        product_link = page.eval_on_selector("li.product-base a", "a => a.href")
        print(f"Visiting product page: {product_link}")
        
        page.goto(product_link, wait_until="networkidle")
        
        # Scroll to reviews or click on reviews if needed
        try:
            page.wait_for_selector("a.detailed-reviews-allReviews", timeout=5000)
            page.click("a.detailed-reviews-allReviews")
            page.wait_for_load_state("networkidle")
        except:
            print("Could not find 'All Reviews' link, scrolling instead...")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(5000)

        print("\nCaptured API Requests:")
        for req in api_requests:
            if "myntra.com" in req["url"]:
                print(f"URL: {req['url']}")
                print(f"Method: {req['method']}")
                # print(f"Headers: {json.dumps(req['headers'], indent=2)}")
                if "x-myntra-app-id" in req["headers"]:
                    print(f"x-myntra-app-id: {req['headers']['x-myntra-app-id']}")
                print("-" * 40)

        browser.close()

if __name__ == "__main__":
    run()
