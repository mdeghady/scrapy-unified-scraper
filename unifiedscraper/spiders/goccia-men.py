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
        "Collection": response.css(product_schema['Collection']).get(),
        "sku": response.css(product_schema['sku']).get(),
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

        yield product
