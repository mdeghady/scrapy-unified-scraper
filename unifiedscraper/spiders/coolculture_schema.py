import re

from unifiedscraper.spiders.base_scraper import NextPageScraper, DataCleanser


class Coolculture(NextPageScraper , DataCleanser):
    """Spider for GRS store"""

    name = "coolculture"
    allowed_domains = ["www.coolculture.it"]
    start_urls = ["https://www.coolculture.it/"]

    def parse_product_page(self, response):
        """Parse product page and extract product data"""
        product_schema = self.schema['product_page_schema']
        product = {
        "Brand": response.css(product_schema['Brand']).get(),
        "ProductName": response.css(product_schema['ProductName']).get(),
        "ProductImage": response.css(product_schema['ProductImage']).get(),
        "ProductColor": response.css(product_schema['ProductColor']).get(),
        "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
        "OriginalPrice": response.css(product_schema['OldPrice']).get(),
        "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
        "Category": response.css(product_schema['Category']).get(),
        "sku": response.css(product_schema['sku']).get(),
        "ProductURL": response.url,
        }

        # Clean up the data
        product['PriceCurrency'] = self._convert_currency_symbols_to_code(product['CurrentPrice'])
        product['OriginalPrice'] = float(
            re.search(r'[\d.]+', product['OriginalPrice']).group())
        product['CurrentPrice'] = float(
            re.search(r'[\d.]+', product['CurrentPrice']).group())

        yield product

