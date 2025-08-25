import re

from .base_scraper import NextPageScraper, DataCleanser


class GocciaMen(NextPageScraper, DataCleanser):
    """Spider for GRS store"""
    name = "goccia-men"
    allowed_domains = ["goccia.shop"]
    start_urls = ["https://goccia.shop/"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
        "Brand": response.css(product_schema['Brand']).get(),
        "ProductName": response.css(product_schema['ProductName']).get(),
        "ProductImage": response.css(product_schema['ProductImage']).get(),
        "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
        "Category": response.css(product_schema['Category']).get(),
        "ProductCode" : response.css(product_schema['ProductCode']).get(),
        "sku": response.css(product_schema['sku']).get(),
        "Department": response.css(product_schema['Department']).get(),
        "ProductURL": response.url,
        }
        # Clean the product data
        no_discount_price = response.css(product_schema['NoDiscountPrice']).get()
        if no_discount_price:
            product['CurrentPrice'] = no_discount_price
            product['OriginalPrice'] = no_discount_price
        else:
            product['CurrentPrice'] = response.css(product_schema['CurrentPrice']).get()
            product['OriginalPrice'] = response.css(product_schema['OldPrice']).get()
        product["PriceCurrency"] = self._convert_currency_symbols_to_code(
            response.css(product_schema['PriceCurrency']).get())
        product['OriginalPrice'] = float(
            re.search(r'[\d.,]+', product['OriginalPrice']).group().replace('.' , "").replace(',', '.'))
        product['CurrentPrice'] = float(
            re.search(r'[\d.,]+', product['CurrentPrice']).group().replace('.' , "").replace(',', '.'))

        product_code_splitted = product['ProductCode'].split('-')
        product["ProductColor"] = product_code_splitted[1] if len(product_code_splitted) > 1 else None

        yield product
