import re

from .base_scraper import NextPageScraper , DataCleanser

class TendenzeSpider(NextPageScraper,DataCleanser):
    """Spider for Tendenze store"""
    name = "tendenze"
    allowed_domains = ["tendenzestore.com"]
    start_urls = ["https://www.tendenzestore.com/en/"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
            "Brand": response.css(product_schema['Brand']).get(),
            "ProductName": response.css(product_schema['ProductName']).get(),
            "ProductImage": response.css(product_schema['ProductImage']).get(),
            "sku": response.css(product_schema['sku']).get(),
            "Category": response.css(product_schema['Category']).get(),
            "ProductURL": response.url,
        }

        product["ProductImage"] = self.make_absolute_url(product["ProductImage"])

        available_sizes = response.css(product_schema['AvailableSizes.option1']).getall()
        if not available_sizes:
            available_sizes = response.css(product_schema['AvailableSizes.option2']).getall()
        product['AvailableSizes'] = available_sizes
        product["sku"]  = product["sku"].split(" ")[-1]
        sku_splitted = product["sku"].split("/")
        product['ProductCode'] = sku_splitted[0]
        product['ProductColor'] = sku_splitted[1] if len(sku_splitted) > 1 else ""

        discount_price = response.css(product_schema['CurrentPrice']).get()
        if discount_price:
            product['CurrentPrice'] = discount_price
            product["OriginalPrice"] = response.css(product_schema['OldPrice']).get()
        else:
            product['CurrentPrice'] = response.css(product_schema['PriceNoDiscount']).get()
            product["OriginalPrice"] = response.css(product_schema['PriceNoDiscount']).get()
        product["PriceCurrency"] = self._convert_currency_symbols_to_code(product['CurrentPrice'])

        yield product