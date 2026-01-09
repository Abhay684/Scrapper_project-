import requests

product_id = "10779712"
endpoints = [
    f"https://www.myntra.com/gateway/v2/product/{product_id}/reviews",
    f"https://www.myntra.com/api/v1/reviews/{product_id}",
    f"https://www.myntra.com/v2/review/{product_id}",
    f"https://www.myntra.com/gateway/v1/product/{product_id}/reviews",
    f"https://www.myntra.com/gateway/v2/reviews/{product_id}",
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "x-myntra-app-id": "pc" # Common app id for desktop
}

for url in endpoints:
    try:
        print(f"Testing: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("Success!")
            print(response.text[:200])
            break
    except Exception as e:
        print(f"Error: {e}")
