import re

from .base_scraper import NextPageScraper, DataCleanser


class Bagaglio(NextPageScraper, DataCleanser):
    """Spider for Cisalfa store"""
    name = "bagaglio"
    allowed_domains = ["bagaglio.it"]
    start_urls = ["https://www.bagaglio.it/"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
        "Brand": response.css(product_schema['Brand']).get(),
        "ProductName": response.css(product_schema['ProductName']).get(),
        "ProductImage": response.css(product_schema['ProductImage']).get(),
        "ProductColor" : response.css(product_schema['ProductColor']).get(),
        "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
        "OriginalPrice": response.css(product_schema['OldPrice']).get(),
        "Category": response.css(product_schema['Category']).get(),
        "sku": response.css(product_schema['sku']).get(),
        "AvailableSizes": "One Size",
        "ProductURL": response.url,
        }

        # Clean Data
        product['PriceCurrency'] = self._convert_currency_symbols_to_code(product['CurrentPrice'])
        product['CurrentPrice'] = float(
            re.search(r'[\d,]+', product['CurrentPrice']).group().replace(',', '.'))

        if product['OriginalPrice']:
            product['OriginalPrice'] = float(
                re.search(r'[\d,]+', product['OriginalPrice']).group().replace(',', '.'))
        else:
            product['OriginalPrice'] = product['CurrentPrice']

        product['ProductColor'] = product['ProductColor'].replace("\n" , "").strip()

        attributes = {}
    
        # Extract all <li> elements inside <ul class="attributes">
        for li in response.css('ul.attributes li'):
            key = li.css('span.attrTitle::text, label.attrTitle::text').get()
            value = li.css('span.attrValue::text').get()
            
            if key and value:
                key = key.strip().rstrip(':')  # Clean up key (remove trailing colon)
                value = value.strip()
                attributes[key] = value

        product['Description'] = attributes
        yield product
