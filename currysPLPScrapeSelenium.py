import time
import random
import csv
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
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
            time.sleep(random.uniform(2, 4))
            page_source = driver.page_source
            logger.info(f"Page source retrieved, length: {len(page_source)} characters")
            soup = BeautifulSoup(page_source, 'html.parser')
            logger.info("Page parsed with BeautifulSoup")
            return soup
        except Exception as e:
            logger.error(f"Error fetching {url} (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(random.uniform(2, 5))
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None

def scrape_product_info(product):
    logger.info("Scraping product information")
    try:
        name = product.find('h2', class_='pdp-grid-product-name')
        price = product.find('span', class_='value')
        link = product.find('a', href=True)
        rating = product.find('span', class_='nvda_star_reading')
        reviews = product.find('span', class_='rating-count average-reviews')
        
        if name and price and link:
            name_text = name.text.strip()
            price_text = price.text.strip()
            product_url = link['href']
            if not product_url.startswith('http'):
                product_url = 'https://www.currys.co.uk' + product_url
            rating_text = rating.text.strip() if rating else 'No rating'
            reviews_text = reviews.text.strip() if reviews else 'No reviews'
            
            logger.info(f"Found product: {name_text}, Price: {price_text}, URL: {product_url}, Rating: {rating_text}, Reviews: {reviews_text}")
            return {
                'name': name_text,
                'price': price_text,
                'url': product_url,
                'rating': rating_text,
                'reviews': reviews_text
            }
        else:
            logger.warning("Product name, price, or URL not found")
            return None
    except AttributeError as e:
        logger.error(f"Error parsing product: {e}")
        return None

def scrape_page(url, driver):
    logger.info(f"Scraping page: {url}")
    soup = get_soup(url, driver)
    if not soup:
        logger.error("No soup object returned, skipping page")
        return [], None

    product_grid = soup.find('div', class_='row product-grid list-view justify-content-center')
    if not product_grid:
        logger.warning("No product grid found on page")
        products = []
    else:
        products = product_grid.find_all('div', class_='row plp-list-grid')
        logger.info(f"Found {len(products)} products on page")
    
    product_data = []
    for product in products:
        info = scrape_product_info(product)
        if info:
            product_data.append(info)
    
    logger.info(f"Collected {len(product_data)} valid products from page")
    
    next_link = soup.find('a', class_='next')
    next_url = next_link['href'] if next_link else None
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
            if next_path.startswith('http'):
                current_url = next_path
            else:
                current_url = 'https://www.currys.co.uk' + next_path
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
    with open('apple_products.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'price', 'url', 'rating', 'reviews'])
        writer.writeheader()
        writer.writerows(all_products)
    
    logger.info(f"Scraped {len(all_products)} products from all categories. Data saved to apple_products.csv")

if __name__ == "__main__":
    main()