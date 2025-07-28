import time
import random
import csv
import logging
import json
import requests
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_free_proxies():
    """Fetch and validate free proxies from proxyscrape.com."""
    logger.info("Fetching free proxies from proxyscrape.com")
    proxy_urls = [
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&simplified=true",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=https&timeout=10000&country=all&simplified=true",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all&simplified=true"
    ]
    proxies = []
    
    for url in proxy_urls:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                proxy_list = response.text.strip().split('\n')
                proxies.extend([f"{proxy}:http" if "http" in url else f"{proxy}:socks5" for proxy in proxy_list if proxy])
                logger.info(f"Fetched {len(proxy_list)} proxies from {url}")
        except Exception as e:
            logger.error(f"Error fetching proxies from {url}: {e}")
    
    # Validate proxies
    valid_proxies = []
    test_url = "https://www.google.com"
    for proxy in proxies[:100]:  # Limit to first 100 to speed up validation
        try:
            proxy_type, proxy_addr = proxy.split(':')
            proxy_dict = {
                'http': f"{proxy_type}://{proxy_addr}",
                'https': f"{proxy_type}://{proxy_addr}"
            }
            response = requests.get(test_url, proxies=proxy_dict, timeout=5)
            if response.status_code == 200:
                valid_proxies.append(proxy)
                logger.info(f"Validated proxy: {proxy}")
        except Exception:
            continue
    
    logger.info(f"Found {len(valid_proxies)} valid proxies")
    return valid_proxies if valid_proxies else proxies[:50]  # Fallback to unvalidated proxies if none pass

def setup_driver(proxy=None):
    """Set up Chrome driver with optional proxy."""
    logger.info("Setting up Chrome driver")
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1'
    ]
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Comment out for debugging
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
    chrome_options.add_argument('accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
    chrome_options.add_argument('accept-language=en-US,en;q=0.5')
    chrome_options.add_argument('accept-encoding=gzip, deflate, br')

    proxy_options = {}
    if proxy:
        proxy_type, proxy_addr = proxy.split(':')
        proxy_options = {
            'proxy': {
                'http': f"{proxy_type}://{proxy_addr}",
                'https': f"{proxy_type}://{proxy_addr}",
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        logger.info(f"Using proxy: {proxy}")

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options,
            seleniumwire_options=proxy_options
        )
        logger.info("Chrome driver initialized")
    except Exception as e:
        logger.error(f"Error initializing driver: {e}")
        raise
    return driver

def get_soup(url, driver, proxies, retries=5):
    """Fetch page content with proxy cycling."""
    logger.info(f"Fetching URL: {url}")
    current_proxies = proxies.copy()
    random.shuffle(current_proxies)
    
    for attempt in range(retries):
        proxy = current_proxies[attempt % len(current_proxies)] if current_proxies else None
        try:
            if attempt > 0:  # Restart driver with new proxy on retry
                driver.quit()
                driver = setup_driver(proxy)
            driver.get(url)
            if "cloudflare" in driver.page_source.lower() or "sorry, you have been blocked" in driver.page_source.lower():
                logger.error(f"Cloudflare block detected on {url} with proxy {proxy}")
                if current_proxies:
                    current_proxies.remove(proxy)  # Remove failed proxy
                continue
            try:
                cookie_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id*='cookie'], button[class*='cookie'], a[class*='cookie']"))
                )
                cookie_button.click()
                logger.info("Accepted cookies")
                time.sleep(random.uniform(1, 2))
            except:
                logger.info("No cookie button found")
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    "div[class*='product-grid'], div.product, div[class*='product-list'], div[class*='product-card'], div[class*='results']"
                ))
            )
            for _ in range(3):  # Multiple "load more" clicks
                try:
                    load_more = driver.find_element(By.CSS_SELECTOR, "button[class*='load-more'], a[class*='load-more']")
                    if load_more and load_more.is_displayed():
                        load_more.click()
                        logger.info("Clicked load more button")
                        time.sleep(random.uniform(2, 3))
                    else:
                        break
                except:
                    logger.info("No load more button found")
                    break
            for _ in range(3):  # Multiple scrolls
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(1, 2))
            page_source = driver.page_source
            logger.info(f"Page source retrieved, length: {len(page_source)} characters")
            soup = BeautifulSoup(page_source, 'html.parser')
            logger.info("Page parsed with BeautifulSoup")
            return soup
        except Exception as e:
            logger.error(f"Error fetching {url} (attempt {attempt + 1}/{retries}): {e}")
            url_last_part = url.split('/')[-1].replace('/', '_')
            filename = f'failed_page_{attempt + 1}_{url_last_part}.html'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logger.info(f"Saved failed page source to {filename}")
            if current_proxies and proxy:
                current_proxies.remove(proxy)  # Remove failed proxy
            time.sleep(random.uniform(5, 10))
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None

def flatten_product_data(data, product_url, rating_text, reviews_text):
    """Flatten the JSON data-productdatalayer into a dictionary for CSV."""
    try:
        flat_data = {
            'title': 'No title',
            'price_revenue': 'No price',
            'product_code': 'No product code',
            'rating': rating_text,
            'reviews': reviews_text,
            'url': product_url,
            'brand': 'No brand',
            'ean': 'No EAN',
            'sku': 'No SKU',
            'price_base_revenue': 'No base revenue',
            'price_currency': 'No currency',
            'price_tax': 'No tax',
            'price_offers': 'No offers',
            'payment_one_off_amount': 'No one-off amount',
            'payment_monthly_amount': 'No monthly amount',
            'availability_shipping_status': 'No shipping status',
            'availability_collect_status': 'No collect status',
            'availability_shipping_type': 'No shipping type',
            'availability_collect_type': 'No collect type',
            'category_categories': 'No categories',
            'category_merchendising_area': 'No merchendising area',
            'category_sub_planning_group': 'No sub planning group',
            'category_planning_group': 'No planning group',
            'category_product_type': 'No product type'
        }
        flat_data['title'] = data.get('name', 'No title')
        flat_data['product_code'] = data.get('id', 'No product code')
        flat_data['brand'] = data.get('brand', 'No brand')
        flat_data['ean'] = data.get('ean', 'No EAN')
        flat_data['sku'] = data.get('sku', 'No SKU')
        price = data.get('price', [{}])[0]
        flat_data['price_revenue'] = str(price.get('revenue', 'No price'))
        flat_data['price_base_revenue'] = str(price.get('baseRevenue', 'No base revenue'))
        flat_data['price_currency'] = price.get('currency', 'No currency')
        flat_data['price_tax'] = str(price.get('tax', 'No tax'))
        flat_data['price_offers'] = ', '.join([offer.get('name', '') for offer in price.get('offer', [])]) or 'No offers'
        for payment in data.get('payment', []):
            if payment.get('frequency') == 'one off':
                flat_data['payment_one_off_amount'] = str(payment.get('amount', 'No one-off amount'))
            elif payment.get('frequency') == 'monthly':
                flat_data['payment_monthly_amount'] = str(payment.get('amount', 'No monthly amount'))
        for avail in data.get('availability', []):
            if avail.get('availabilityStatus') == 'shipping':
                flat_data['availability_shipping_status'] = avail.get('availabilityStatus', 'No shipping status')
                flat_data['availability_shipping_type'] = avail.get('availabilityType', 'No shipping type')
            elif avail.get('availabilityStatus') == 'collect in store':
                flat_data['availability_collect_status'] = avail.get('availabilityStatus', 'No collect status')
                flat_data['availability_collect_type'] = avail.get('availabilityType', 'No collect type')
        category = data.get('category', {})
        flat_data['category_categories'] = ', '.join(category.get('categories', [])) or 'No categories'
        flat_data['category_merchendising_area'] = category.get('merchendisingArea', 'No merchendising area')
        flat_data['category_sub_planning_group'] = category.get('subPlanningGroup', 'No sub planning group')
        flat_data['category_planning_group'] = category.get('planningGroup', 'No planning group')
        flat_data['category_product_type'] = category.get('productType', 'No product type')
        return flat_data
    except Exception as e:
        logger.error(f"Error flattening product data: {e}")
        return None

def scrape_product_info(product):
    """Extract product information from a product element."""
    logger.info("Scraping product information")
    try:
        data_layer = product.get('data-productdatalayer')
        if not data_layer:
            logger.warning("No data-productdatalayer found for product")
            return None
        try:
            data = json.loads(data_layer)[0]
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing data-productdatalayer: {e}")
            return None
        link = product.find('a', class_='link text-truncate pdpLink', href=True)
        product_url = 'https://www.currys.co.uk' + link['href'] if link and not link['href'].startswith('http') else link['href'] if link else 'No URL'
        rating = product.find('span', class_='nvda_star_reading')
        reviews = product.find('span', class_='rating-count average-reviews')
        rating_text = rating.text.strip() if rating else 'No rating'
        reviews_text = reviews.text.strip() if reviews else 'No reviews'
        flat_data = flatten_product_data(data, product_url, rating_text, reviews_text)
        if not flat_data:
            logger.warning(f"Failed to flatten data for product: {product_url}")
            return None
        if flat_data['title'] != 'No title' and flat_data['price_revenue'] != 'No price':
            logger.info(f"Scraped product: {flat_data['title']}, Price: {flat_data['price_revenue']}, Code: {flat_data['product_code']}, Rating: {flat_data['rating']}, Reviews: {flat_data['reviews']}, URL: {flat_data['url']}")
            return flat_data
        else:
            logger.warning(f"Missing title or price for product: {product_url}")
            return None
    except Exception as e:
        logger.error(f"Error parsing product: {e}")
        return None

def scrape_page(url, driver, proxies):
    """Scrape a single page and handle pagination."""
    logger.info(f"Scraping lister page: {url}")
    soup = get_soup(url, driver, proxies)
    if not soup:
        logger.error("No soup object returned, skipping page")
        return [], None
    product_grid_selectors = [
        'div[class*="product-grid"]',
        'div[class*="product-list"]',
        'div[class*="products"]',
        'div[class*="results"]'
    ]
    products = []
    for selector in product_grid_selectors:
        product_grid = soup.select_one(selector)
        if product_grid:
            products = product_grid.find_all('div', class_='product')
            logger.info(f"Found {len(products)} products using selector: {selector}")
            break
    if not products:
        logger.warning("No product grid found, trying fallback selectors")
        products = soup.find_all('div', class_='product') or \
                  soup.find_all('div', attrs={'data-productdatalayer': True}) or \
                  soup.find_all('div', class_='product-card')
        logger.info(f"Found {len(products)} products using fallback selectors")
    product_data = []
    for product in products:
        info = scrape_product_info(product)
        if info:
            product_data.append(info)
    logger.info(f"Collected {len(product_data)} valid products from page")
    next_link_selectors = ['a.next', 'a[class*="next-page"]', 'a[rel="next"]', 'a[class*="pagination-next"]']
    next_url = None
    for selector in next_link_selectors:
        next_link = soup.select_one(selector)
        if next_link and 'href' in next_link.attrs:
            next_url = next_link['href']
            break
    if next_url and not next_url.startswith('http'):
        next_url = 'https://www.currys.co.uk' + next_url
    logger.info(f"Next page link: {next_url if next_url else 'None'}")
    return product_data, next_url

def scrape_category(category_url, driver, proxies):
    """Scrape all pages in a category."""
    logger.info(f"Starting to scrape category: {category_url}")
    all_products = []
    current_url = category_url
    while current_url:
        products, next_path = scrape_page(current_url, driver, proxies)
        all_products.extend(products)
        logger.info(f"Total products collected in category so far: {len(all_products)}")
        if next_path:
            current_url = next_path
            logger.info(f"Moving to next page: {current_url}")
            time.sleep(random.uniform(5, 10))
        else:
            logger.info("No more pages in category")
            current_url = None
    logger.info(f"Finished scraping category, collected {len(all_products)} products")
    return all_products

def main():
    """Main function to scrape all categories."""
    category_urls = [
        'https://www.currys.co.uk/computing/desktop-pcs/desktops/apple',
        'https://www.currys.co.uk/computing/laptops/laptops/apple',
        'https://www.currys.co.uk/phones/mobile-phones/mobile-phones/apple',
        'https://www.currys.co.uk/smart-tech/smart-watches-and-fitness/smart-watches/apple',
        'https://www.currys.co.uk/computing/ipad-tablets-and-ereaders/tablets/apple',
        'https://www.currys.co.uk/phones/mobile-phone-accessories/mobile-phone-accessories/apple'
    ]
    logger.info("Starting scraper")
    proxies = fetch_free_proxies()
    if not proxies:
        logger.warning("No proxies available, proceeding without proxies")
    driver = setup_driver()
    
    all_products = []
    try:
        for category_url in category_urls:
            logger.info(f"Processing category: {category_url}")
            if not proxies:
                logger.info("Refetching proxies due to depletion")
                proxies = fetch_free_proxies()
            products = scrape_category(category_url, driver, proxies)
            all_products.extend(products)
            with open('apple_products_dataLayer.csv', 'a', newline='', encoding='utf-8-sig') as f:
                fieldnames = [
                    'title', 'price_revenue', 'product_code', 'rating', 'reviews', 'url',
                    'brand', 'ean', 'sku', 'price_base_revenue', 'price_currency', 'price_tax',
                    'price_offers', 'payment_one_off_amount', 'payment_monthly_amount',
                    'availability_shipping_status', 'availability_collect_status',
                    'availability_shipping_type', 'availability_collect_type',
                    'category_categories', 'category_merchendising_area',
                    'category_sub_planning_group', 'category_planning_group', 'category_product_type'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if f.tell() == 0:  # Write header if file is empty
                    writer.writeheader()
                writer.writerows(products)
            logger.info(f"Wrote {len(products)} products from {category_url} to CSV")
            logger.info(f"Total products collected across all categories: {len(all_products)}")
            time.sleep(random.uniform(5, 10))
    finally:
        logger.info("Closing Chrome driver")
        driver.quit()
    
    logger.info(f"Scraped {len(all_products)} products from all categories. Data saved to apple_products_dataLayer.csv")

if __name__ == "__main__":
    main()