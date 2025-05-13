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
        "CurrentPrice": response.css(product_schema['CurrentPrice']).getall()[0],
        "OriginalPrice": response.css(product_schema['OldPrice']).getall()[0],
        "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
        "Category": response.css(product_schema['Category']).get(),
        "Collection": response.css(product_schema['Collection']).get(),
        "sku": response.css(product_schema['sku']).get(),
        }
        # Clean the product data

        yield product
