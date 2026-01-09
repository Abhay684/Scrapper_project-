import re
import json
from playwright.sync_api import sync_playwright

def test_api():
    product_id = "22258458" # Jockey Hipster
    url = f"https://www.myntra.com/gateway/v1/reviews/product/{product_id}?size=10&sort=1&rating=0&page=1&includeMetaData=true"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Use request context
        request_context = browser.new_context().request
        
        print(f"Fetching API: {url}")
        response = request_context.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json"
        })
        
        if not response.ok:
            print(f"Error: {response.status} {response.status_text}")
            return
            
        data = response.json()
        print(json.dumps(data, indent=2)[:1000])
        
        print(json.dumps(data, indent=2)[:1000])
        
        if "reviews" in data and data["reviews"]:
            print(f"Found {len(data['reviews'])} reviews")
            print("First review date field:", data["reviews"][0].get("date") or data["reviews"][0].get("createdOn"))
        else:
            print("No reviews found or error")
            
        browser.close()

if __name__ == "__main__":
    test_api()
