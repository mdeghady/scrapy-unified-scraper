import re

from .base_scraper import NextPageScraper, DataCleanser


class Cisalfa(NextPageScraper, DataCleanser):
    """Spider for Cisalfa store"""
    name = "cisalfa"
    allowed_domains = ["cisalfasport.it"]
    start_urls = ["https://www.cisalfasport.it/it-it/"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
        "Brand": response.css(product_schema['Brand']).get(),
        "ProductName": response.css(product_schema['ProductName']).get(),
        "ProductImage": response.css(product_schema['ProductImage']).get(),
        "ProductColor" : response.css(product_schema['ProductColor']).get(),
        "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
        "OriginalPrice": response.css(product_schema['OldPrice']).get(),
        "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
        "Category": response.css(product_schema['Category']).get(),
        "Department": response.css(product_schema['Department']).get(),
        "sku": response.css(product_schema['sku']).get(),
        "PriceCurrency": response.css(product_schema['PriceCurrency']).get(),
        "ProductURL": response.url,
        }

        if product['ProductImage']:
            product['ProductImage'] = self.make_absolute_url(
                follow_url=product['ProductImage'],
                parent_url="https://www.cisalfasport.it/"
            )

        if product["AvailableSizes"]:
            product["AvailableSizes"] = [size.replace("," , ".") for size in product["AvailableSizes"] if size]

        product['ProductColor'] = product['ProductColor'].split("-")[-1].strip() if product['ProductColor'] else None

        yield product
