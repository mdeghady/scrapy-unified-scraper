import json
import re

from unifiedscraper.spiders.base_scraper import NextPageScraper, DataCleanser


class Evolution(NextPageScraper , DataCleanser):
    """Spider for Deflorio store"""

    name = "evolution"
    allowed_domains = ["www.evolutionsessa.com"]
    start_urls = ["https://www.evolutionsessa.com"]

    def parse_product_page(self, response):
        """Parse product page and extract product data"""
        product_schema = self.schema['product_page_schema']
        product = {
            "Brand": response.css(product_schema['Brand']).get(),
            "ProductName": response.css(product_schema['ProductName']).get(),
            "ProductImage": response.css(product_schema['ProductImage']).get(),
            "ProductDetails": response.css(product_schema['ProductDetails']).get(),
            "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
            "OriginalPrice": response.css(product_schema['OldPrice']).get(),
            "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
            "Category": response.css(product_schema['Category']).get(),
            "sku": response.css(product_schema['sku']).get(),
            "ProductURL": response.url,
        }

        # Clean the data
        #clean price data
        product['PriceCurrency'] = self._convert_currency_symbols_to_code(product['CurrentPrice'])
        product['CurrentPrice'] = float(
            re.search(r'[\d.]+', product['CurrentPrice']).group())
        if product['OriginalPrice']:
            product['OriginalPrice'] = float(
                re.search(r'[\d.]+', product['OriginalPrice']).group())
        else:
            product['OriginalPrice'] = product['CurrentPrice']


        #clean sku data
        product['sku'] = product['sku'].replace("SKU:" , "")


        try:
            product_details = json.loads(product.pop('ProductDetails'))
            product['ProductColor'] = product_details.get('color')
            product['StockAvailability'] = product_details['offers']['offers'][0]['StockAvailability']
        except:
            product['ProductColor'] = None
            product['StockAvailability'] = None

        yield product
