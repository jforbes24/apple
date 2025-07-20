import time
import random
import csv
import logging
import json
from selenium import webdriver
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

def setup_driver():
    logger.info("Setting up Chrome driver")
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Run in headless mode
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    chrome_options.add_argument('accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
    chrome_options.add_argument('accept-language=en-US,en;q=0.5')
    chrome_options.add_argument('accept-encoding=gzip, deflate, br')
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        logger.info("Chrome driver initialized with newer Selenium syntax")
    except TypeError:
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), chrome_options=chrome_options)
        logger.info("Chrome driver initialized with older Selenium syntax (fallback)")
    return driver

def get_soup(url, driver, retries=3):
    logger.info(f"Fetching URL: {url}")
    for attempt in range(retries):
        try:
            driver.get(url)
            logger.info(f"Waiting for page to load (attempt {attempt + 1}/{retries})")
            # Wait for product grid or product elements
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='product-grid'], div.product"))
            )
            # Scroll to trigger JavaScript rendering
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2, 4))  # Additional delay for content to settle
            page_source = driver.page_source
            logger.info(f"Page source retrieved, length: {len(page_source)} characters")
            soup = BeautifulSoup(page_source, 'html.parser')
            logger.info("Page parsed with BeautifulSoup")
            return soup
        except Exception as e:
            logger.error(f"Error fetching {url} (attempt {attempt + 1}/{retries}): {e}")
            # Compute filename component outside f-string
            url_last_part = url.split('/')[-1].replace('/', '_')  # Replace any slashes for valid filename
            filename = f'failed_page_{attempt + 1}_{url_last_part}.html'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logger.info(f"Saved failed page source to {filename}")
            time.sleep(random.uniform(2, 5))
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None

def flatten_product_data(data, product_url, rating_text, reviews_text):
    """Flatten the JSON data-productdatalayer into a dictionary for CSV."""
    try:
        # Initialize default values
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

        # Core product info
        flat_data['title'] = data.get('name', 'No title')
        flat_data['product_code'] = data.get('id', 'No product code')
        flat_data['brand'] = data.get('brand', 'No brand')
        flat_data['ean'] = data.get('ean', 'No EAN')
        flat_data['sku'] = data.get('sku', 'No SKU')

        # Price info
        price = data.get('price', [{}])[0]
        flat_data['price_revenue'] = str(price.get('revenue', 'No price'))
        flat_data['price_base_revenue'] = str(price.get('baseRevenue', 'No base revenue'))
        flat_data['price_currency'] = price.get('currency', 'No currency')
        flat_data['price_tax'] = str(price.get('tax', 'No tax'))
        flat_data['price_offers'] = ', '.join([offer.get('name', '') for offer in price.get('offer', [])]) or 'No offers'

        # Payment info
        for payment in data.get('payment', []):
            if payment.get('frequency') == 'one off':
                flat_data['payment_one_off_amount'] = str(payment.get('amount', 'No one-off amount'))
            elif payment.get('frequency') == 'monthly':
                flat_data['payment_monthly_amount'] = str(payment.get('amount', 'No monthly amount'))

        # Availability info
        for avail in data.get('availability', []):
            if avail.get('availabilityStatus') == 'shipping':
                flat_data['availability_shipping_status'] = avail.get('availabilityStatus', 'No shipping status')
                flat_data['availability_shipping_type'] = avail.get('availabilityType', 'No shipping type')
            elif avail.get('availabilityStatus') == 'collect in store':
                flat_data['availability_collect_status'] = avail.get('availabilityStatus', 'No collect status')
                flat_data['availability_collect_type'] = avail.get('availabilityType', 'No collect type')

        # Category info
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
    logger.info("Scraping product information")
    try:
        # Extract data-productdatalayer JSON
        data_layer = product.get('data-productdatalayer')
        if not data_layer:
            logger.warning("No data-productdatalayer found for product")
            return None
        
        # Parse JSON (remove square brackets and parse first object)
        try:
            data = json.loads(data_layer)[0]
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing data-productdatalayer: {e}")
            return None

        # Extract URL from the product link
        link = product.find('a', class_='link text-truncate pdpLink', href=True)
        product_url = 'https://www.currys.co.uk' + link['href'] if link and not link['href'].startswith('http') else link['href'] if link else 'No URL'

        # Extract rating and reviews from HTML
        rating = product.find('span', class_='nvda_star_reading')
        reviews = product.find('span', class_='rating-count average-reviews')
        rating_text = rating.text.strip() if rating else 'No rating'
        reviews_text = reviews.text.strip() if reviews else 'No reviews'

        # Flatten the JSON data
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

def scrape_page(url, driver):
    logger.info(f"Scraping lister page: {url}")
    soup = get_soup(url, driver)
    if not soup:
        logger.error("No soup object returned, skipping page")
        return [], None

    # Try primary product grid selector (used in desktop category)
    product_grid = soup.find('div', class_='row product-grid list-view justify-content-center')
    if product_grid:
        products = product_grid.find_all('div', class_='product')
        logger.info(f"Found {len(products)} products using primary grid selector")
    else:
        logger.warning("Primary product grid not found, trying fallback selector")
        # Fallback: Find all divs with class 'product' or containing data-productdatalayer
        products = soup.find_all('div', class_='product')
        if not products:
            products = soup.find_all('div', attrs={'data-productdatalayer': True})
        logger.info(f"Found {len(products)} products using fallback selector")
    
    product_data = []
    for product in products:
        info = scrape_product_info(product)
        if info:
            product_data.append(info)
    
    logger.info(f"Collected {len(product_data)} valid products from page")
    
    next_link = soup.find('a', class_='next')
    next_url = next_link['href'] if next_link else None
    if next_url and not next_url.startswith('http'):
        next_url = 'https://www.currys.co.uk' + next_url
    logger.info(f"Next page link: {next_url if next_url else 'None'}")
    
    return product_data, next_url

def scrape_category(category_url, driver):
    logger.info(f"Starting to scrape category: {category_url}")
    all_products = []
    current_url = category_url
    while current_url:
        products, next_path = scrape_page(current_url, driver)
        all_products.extend(products)
        logger.info(f"Total products collected in category so far: {len(all_products)}")
        
        if next_path:
            current_url = next_path
            logger.info(f"Moving to next page: {current_url}")
            time.sleep(random.uniform(2, 4))
        else:
            logger.info("No more pages in category")
            current_url = None
    
    logger.info(f"Finished scraping category, collected {len(all_products)} products")
    return all_products

def main():
    # Provided category URLs
    category_urls = [
        'https://www.currys.co.uk/computing/desktop-pcs/desktops/apple',
        'https://www.currys.co.uk/computing/laptops/laptops/apple',
        'https://www.currys.co.uk/phones/mobile-phones/mobile-phones/apple',
        'https://www.currys.co.uk/smart-tech/smart-watches-and-fitness/smart-watches/apple',
        'https://www.currys.co.uk/computing/ipad-tablets-and-ereaders/tablets/apple',
        'https://www.currys.co.uk/phones/mobile-phone-accessories/mobile-phone-accessories/apple'
    ]
    
    logger.info("Starting scraper")
    driver = setup_driver()
    all_products = []
    
    try:
        for category_url in category_urls:
            logger.info(f"Processing category: {category_url}")
            products = scrape_category(category_url, driver)
            all_products.extend(products)
            logger.info(f"Total products collected across all categories: {len(all_products)}")
    finally:
        logger.info("Closing Chrome driver")
        driver.quit()
    
    logger.info(f"Writing {len(all_products)} products to CSV")
    with open('apple_products_dataLayer.csv', 'w', newline='', encoding='utf-8') as f:
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
        writer.writeheader()
        writer.writerows(all_products)
    
    logger.info(f"Scraped {len(all_products)} products from all categories. Data saved to apple_products_dataLayer.csv")

if __name__ == "__main__":
    main()