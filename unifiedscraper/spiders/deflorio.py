import re

from unifiedscraper.spiders.base_scraper import NextPageScraper, DataCleanser


class Deflorio(NextPageScraper , DataCleanser):
    """Spider for Deflorio store"""

    name = "deflorio"
    allowed_domains = ["deflorio1948.it"]
    start_urls = ["https://deflorio1948.it/"]

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

        # Clean the data
        sku_splitted = product['sku'].split('-')
        product['ProductCode'] = sku_splitted[0]
        product['ProductColorCode'] = sku_splitted[1] if len(sku_splitted) > 1 else None

        product['PriceCurrency'] = self._convert_currency_symbols_to_code(product['CurrentPrice'])
        product['CurrentPrice'] = float(
            re.search(r'[\d,]+', product['CurrentPrice']).group().replace(',', '.'))

        if product['OriginalPrice']:
            product['OriginalPrice'] = float(
                re.search(r'[\d,]+', product['OriginalPrice']).group().replace(',', '.'))
        else:
            product['OriginalPrice'] = product['CurrentPrice']

        yield product