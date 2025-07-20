import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_headers():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.currys.co.uk/'
    }

def get_soup(url, retries=3):
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=[403, 429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    try:
        response = session.get(url, headers=get_headers(), timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def scrape_product_info(product):
    try:
        name = product.find('h2', class_='pdp-grid-product-name').text.strip()
        price = product.find('span', class_='value').text.strip()
        return {'name': name, 'price': price}
    except AttributeError:
        return None

def scrape_page(url):
    soup = get_soup(url)
    if not soup:
        return [], None

    products = soup.find_all('article', class_='product')
    product_data = []
    
    for product in products:
        info = scrape_product_info(product)
        if info:
            product_data.append(info)
    
    next_link = soup.find('a', class_='next')
    next_url = next_link['href'] if next_link else None
    
    return product_data, next_url

def scrape_category(category_url):
    all_products = []
    current_url = category_url
    while current_url:
        print(f"Scraping category page: {current_url}")
        products, next_path = scrape_page(current_url)
        all_products.extend(products)
        
        if next_path:
            if next_path.startswith('http'):
                current_url = next_path
            else:
                current_url = 'https://www.currys.co.uk' + next_path
            time.sleep(random.uniform(1, 3))
        else:
            current_url = None
    
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
    
    all_products = []
    for category_url in category_urls:
        print(f"Processing category: {category_url}")
        products = scrape_category(category_url)
        all_products.extend(products)
    
    with open('apple_products.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'price'])
        writer.writeheader()
        writer.writerows(all_products)
    
    print(f"Scraped {len(all_products)} products from all categories. Data saved to apple_products.csv")

if __name__ == "__main__":
    main()