import aiohttp
import asyncio
import pandas as pd
from datetime import datetime
import json
import re
from collections import defaultdict

# Configuration
BASE_URL = "https://www.clovia.com"
CATEGORY_API = f"{BASE_URL}/web/api/v1/category-products-desktop/bras/s/?page="
PRODUCT_REVIEW_API = f"{BASE_URL}/web/api/v1/product/"  # {slug}/web-reviews/s/?page=1
PRODUCT_DETAIL_API = f"{BASE_URL}/web/api/v1/product-desktop/"  # {slug}/

# Headers to mimic browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.clovia.com/",
    "Origin": "https://www.clovia.com"
}

# Semaphore to limit concurrent requests
MAX_CONCURRENT_REQUESTS = 20

class CloviaScraper:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.products_data = []
        self.session = None
        
    async def fetch_json(self, url, content_type_check=True):
        """Fetch JSON from URL with rate limiting"""
        async with self.semaphore:
            try:
                async with self.session.get(url, headers=HEADERS, timeout=30) as response:
                    if response.status == 200:
                        # Read text first, then parse JSON
                        text = await response.text()
                        try:
                            return json.loads(text)
                        except json.JSONDecodeError:
                            # Not valid JSON (probably HTML)
                            return None
                    else:
                        return None
            except Exception as e:
                print(f"Exception fetching {url}: {e}")
                return None
    
    async def get_total_pages(self):
        """Get total number of pages from first API call"""
        url = f"{CATEGORY_API}1"
        data = await self.fetch_json(url)
        if data and data.get("status") == "success":
            total_count = data["result"].get("total_count", 0)
            page_size = data["result"].get("size", 12)
            total_pages = (total_count + page_size - 1) // page_size
            print(f"Total products: {total_count}, Total pages: {total_pages}")
            return total_pages, data["result"].get("products", [])
        return 0, []
    
    async def fetch_all_category_products(self, max_pages=None):
        """Fetch all products from category listing"""
        total_pages, first_page_products = await self.get_total_pages()
        
        if max_pages:
            total_pages = min(total_pages, max_pages)
        
        all_products = first_page_products
        
        # Fetch remaining pages concurrently
        if total_pages > 1:
            tasks = [self.fetch_json(f"{CATEGORY_API}{page}") for page in range(2, total_pages + 1)]
            results = await asyncio.gather(*tasks)
            
            for data in results:
                if data and data.get("status") == "success":
                    products = data["result"].get("products", [])
                    all_products.extend(products)
        
        print(f"Fetched {len(all_products)} products from category listing")
        return all_products
    
    async def fetch_product_details(self, slug):
        """Fetch detailed product information including sold_count"""
        url = f"{PRODUCT_DETAIL_API}{slug}/"
        return await self.fetch_json(url)
    
    async def fetch_product_reviews(self, slug, page=1):
        """Fetch reviews for a product"""
        url = f"{PRODUCT_REVIEW_API}{slug}/web-reviews/s/?page={page}"
        return await self.fetch_json(url)
    
    async def fetch_all_reviews_for_product(self, slug, max_pages=100):
        """Fetch all reviews for a product (paginated)"""
        all_reviews = []
        
        # First page
        first_page = await self.fetch_product_reviews(slug, 1)
        if not first_page:
            return all_reviews
        
        # Handle the actual API response structure
        reviews = first_page.get("object_list", [])
        all_reviews.extend(reviews)
        
        total_reviews = first_page.get("total_reviews", 0)
        num_pages = first_page.get("num_pages", 1)
        has_next = first_page.get("has_next", False)
        
        # Limit pages to avoid too many requests
        total_pages = min(num_pages, max_pages)
        
        # Fetch remaining pages if there are more
        if total_pages > 1 and has_next:
            tasks = [self.fetch_product_reviews(slug, page) for page in range(2, total_pages + 1)]
            results = await asyncio.gather(*tasks)
            
            for data in results:
                if data:
                    reviews = data.get("object_list", [])
                    all_reviews.extend(reviews)
        
        return all_reviews
    
    def parse_review_date(self, date_str):
        """Parse review date string and return datetime object"""
        if not date_str:
            return None
        try:
            # API format: "2025-03-27 22:33:13"
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%b %d, %Y",
                "%d %b %Y",
                "%B %d, %Y",
                "%d %B %Y",
                "%d-%m-%Y",
                "%d/%m/%Y"
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(str(date_str).strip(), fmt)
                except:
                    continue
            
            # Try to extract date from various patterns
            patterns = [
                r'(\d{4})-(\d{2})-(\d{2})',  # 2025-03-27
                r'(\d{1,2})\s*(?:st|nd|rd|th)?\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*,?\s*(\d{4})',
                r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*(\d{1,2})\s*(?:st|nd|rd|th)?\s*,?\s*(\d{4})',
            ]
            
            # Check for YYYY-MM-DD pattern first
            match = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(date_str))
            if match:
                year, month, day = match.groups()
                return datetime(int(year), int(month), int(day))
            
            month_map = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            
            for pattern in patterns[1:]:
                match = re.search(pattern, str(date_str), re.IGNORECASE)
                if match:
                    groups = match.groups()
                    if groups[0].isdigit():
                        day, month_str, year = groups
                    else:
                        month_str, day, year = groups
                    month = month_map.get(month_str.lower()[:3], 1)
                    return datetime(int(year), month, int(day))
            
            return None
        except Exception as e:
            return None
    
    def count_reviews_by_year_month(self, reviews):
        """Count reviews by year and by month for 2024"""
        year_counts = defaultdict(int)
        month_2025_counts = defaultdict(int)
        
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        for review in reviews:
            # Handle array format from API: [id, title, content, product_id, user_id, verified, name, date, ...]
            # The date is at index 7 in the array
            if isinstance(review, list) and len(review) > 7:
                date_str = review[7]  # Date is at index 7
            elif isinstance(review, dict):
                date_str = review.get("created_at", "") or review.get("date", "") or review.get("review_date", "")
            else:
                continue
            
            parsed_date = self.parse_review_date(date_str)
            
            if parsed_date:
                year = parsed_date.year
                year_counts[year] += 1
                
                if year == 2025:
                    month_idx = parsed_date.month - 1
                    month_2025_counts[months[month_idx]] += 1
        
        return year_counts, month_2025_counts
    
    def extract_size_counts(self, product_detail):
        """Extract size availability counts from product detail"""
        size_counts = {
            'S': 0, 'M': 0, 'L': 0, 'XL': 0, '2XL': 0, '3XL': 0, '4XL': 0
        }
        
        if not product_detail:
            return size_counts
        
        result = product_detail.get("result", {})
        
        # Try different possible structures for size data
        sizes = result.get("sizes", []) or result.get("available_sizes", []) or result.get("size_variants", [])
        
        # Also check all_size_prop from listing
        all_size_prop = result.get("all_size_prop", [])
        
        # Map bra sizes to general size categories
        size_mapping = {
            '32': 'S', '34': 'M', '36': 'L', '38': 'XL', 
            '40': '2XL', '42': '3XL', '44': '4XL', '46': '4XL'
        }
        
        for size in sizes:
            if isinstance(size, dict):
                size_name = size.get("size", "") or size.get("name", "")
                qty = size.get("quantity", 1) or size.get("stock", 1) or 1
            else:
                size_name = str(size)
                qty = 1
            
            # Extract band size for mapping
            band_match = re.match(r'(\d{2})', size_name)
            if band_match:
                band = band_match.group(1)
                if band in size_mapping:
                    size_counts[size_mapping[band]] += qty
            
            # Direct size matching
            for sz in size_counts.keys():
                if sz in size_name.upper():
                    size_counts[sz] += qty
                    break
        
        return size_counts
    
    async def process_product(self, product, fetch_reviews=True, fetch_details=True):
        """Process a single product and extract all required data"""
        slug = product.get("slug", "")
        
        # Basic product info from listing
        product_data = {
            "Brand Name": "Clovia",
            "Full Name": product.get("name", "") or product.get("translated_name", ""),
            "Price": product.get("rounded_up_unit_price_ui", 0) or product.get("rounded_up_unit_price", 0),
            "Product Rating": product.get("star_rating", 0),
            "Customer Reviews Count": product.get("review_count", 0),
            "Ratings Count": 0,
            "Sold Count": 0,
            "Product URL": f"{BASE_URL}/product/{slug}/" if slug else "",
            "SKU": product.get("sku", ""),
        }
        
        # Initialize year and month counts
        for year in range(2020, 2027):
            product_data[f"{year} Review Count"] = 0
        
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        for month in months:
            product_data[f"2025_{month}"] = 0
        
        # Fetch detailed reviews if enabled and there are reviews
        if fetch_reviews and product.get("review_count", 0) > 0 and slug:
            reviews = await self.fetch_all_reviews_for_product(slug)
            if reviews:
                year_counts, month_2025_counts = self.count_reviews_by_year_month(reviews)
                
                for year, count in year_counts.items():
                    if 2020 <= year <= 2026:
                        product_data[f"{year} Review Count"] = count
                
                for month, count in month_2025_counts.items():
                    product_data[f"2025_{month}"] = count
        
        # Fetch product details for sold_count and ratings count
        if slug:
            details = await self.fetch_product_details(slug)
            if details:
                # Extract sold_count from rvp object
                rvp = details.get("rvp", {})
                if rvp:
                    product_data["Sold Count"] = rvp.get("sold_count", 0) or 0
                
                # Extract total_ratings (first element of the array)
                total_ratings = details.get("total_ratings", [])
                if total_ratings and isinstance(total_ratings, list) and len(total_ratings) > 0:
                    first_rating = total_ratings[0]
                    if isinstance(first_rating, (int, float)):
                        product_data["Ratings Count"] = int(first_rating)
        
        return product_data
    
    async def scrape_all(self, max_pages=None, fetch_reviews=True, fetch_details=False):
        """Main scraping function"""
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, limit_per_host=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session
            
            print("Fetching product listings...")
            products = await self.fetch_all_category_products(max_pages)
            
            print(f"Processing {len(products)} products...")
            
            # Process products in batches for progress tracking
            batch_size = 50
            all_product_data = []
            
            for i in range(0, len(products), batch_size):
                batch = products[i:i+batch_size]
                tasks = [self.process_product(p, fetch_reviews, fetch_details) for p in batch]
                batch_results = await asyncio.gather(*tasks)
                all_product_data.extend(batch_results)
                print(f"Processed {min(i+batch_size, len(products))}/{len(products)} products")
            
            self.products_data = all_product_data
            return all_product_data
    
    def save_to_excel(self, filename="clovia_products.xlsx"):
        """Save scraped data to Excel file"""
        if not self.products_data:
            print("No data to save!")
            return
        
        df = pd.DataFrame(self.products_data)
        
        # Reorder columns
        column_order = [
            "Brand Name", "Full Name", "Price", "Product Rating", "Customer Reviews Count", "Ratings Count", "Sold Count", "Product URL", "SKU",
            "2020 Review Count", "2021 Review Count", "2022 Review Count", 
            "2023 Review Count", "2024 Review Count", "2025 Review Count", "2026 Review Count",
            "2025_JAN", "2025_FEB", "2025_MAR", "2025_APR", "2025_MAY", "2025_JUN",
            "2025_JUL", "2025_AUG", "2025_SEP", "2025_OCT", "2025_NOV", "2025_DEC"
        ]
        
        # Ensure all columns exist
        for col in column_order:
            if col not in df.columns:
                df[col] = 0
        
        df = df[column_order]
        
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"Data saved to {filename}")
        return df
    
    def save_to_csv(self, filename="clovia_products.csv"):
        """Save scraped data to CSV file"""
        if not self.products_data:
            print("No data to save!")
            return
        
        df = pd.DataFrame(self.products_data)
        
        # Reorder columns
        column_order = [
            "Brand Name", "Full Name", "Price", "Product Rating", "Customer Reviews Count", "Ratings Count", "Sold Count", "Product URL", "SKU",
            "2020 Review Count", "2021 Review Count", "2022 Review Count", 
            "2023 Review Count", "2024 Review Count", "2025 Review Count", "2026 Review Count",
            "2025_JAN", "2025_FEB", "2025_MAR", "2025_APR", "2025_MAY", "2025_JUN",
            "2025_JUL", "2025_AUG", "2025_SEP", "2025_OCT", "2025_NOV", "2025_DEC"
        ]
        
        for col in column_order:
            if col not in df.columns:
                df[col] = 0
        
        df = df[column_order]
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Data saved to {filename}")
        return df


async def main():
    """Main entry point"""
    scraper = CloviaScraper()
    
    print("=" * 60)
    print("CLOVIA PRODUCT SCRAPER")
    print("=" * 60)
    
    # Configuration options:
    # max_pages: Set to None to fetch all pages, or a number to limit (e.g., 5 for testing)
    # fetch_reviews: Set to True to fetch review date breakdowns (slower but more data)
    # fetch_details: Set to True to fetch individual product details for size info
    
    # For quick test (2 pages, with review details):
    # data = await scraper.scrape_all(max_pages=2, fetch_reviews=True, fetch_details=False)
    
    # For full scrape with review breakdowns:
    data = await scraper.scrape_all(max_pages=None, fetch_reviews=True, fetch_details=False)
    
    # Save results
    scraper.save_to_excel("clovia_products.xlsx")
    scraper.save_to_csv("clovia_products.csv")
    
    print("=" * 60)
    print(f"Scraping completed! Total products: {len(data)}")
    print("=" * 60)


if __name__ == "__main__":
    # Run the async scraper
    asyncio.run(main())
