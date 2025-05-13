import re

from .base_scraper import NextPageScraper

class TendenzeSpider(NextPageScraper):
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
            "sku": response.css(product_schema['SkuCode']).get(),
            "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
            "OriginalPrice": response.css(product_schema['OldPrice']).get(),
            "Category": response.css(product_schema['Category']).get(),
            "ProductURL": response.url,
        }

        available_sizes = response.css(product_schema['AvailableSizes.option1']).getall()
        if not available_sizes:
            available_sizes = response.css(product_schema['AvailableSizes.option2']).getall()
        product['AvailableSizes'] = available_sizes
        product["sku"]  = product["sku"].split(" ")[-1]
        sku_splitted = product["sku"].split("/")
        product['ProductCode'] = sku_splitted[0]
        product['ProductColor'] = sku_splitted[1] if len(sku_splitted) > 1 else ""

        yield product