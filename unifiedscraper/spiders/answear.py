import re
import json
from .base_scraper import NextPageScraper, DataCleanser , LoadMoreScrapper


class AnswearUomo(NextPageScraper, DataCleanser):
    """Spider for Answear store Uomo section."""
    name = "answear-uomo"
    allowed_domains = ["answear.it"]
    start_urls = ["https://answear.it"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
        "Brand": response.css(product_schema['Brand']).get(),
        "ProductName": response.css(product_schema['ProductName']).get(),
        "ProductImage": response.css(product_schema['ProductImage']).get(),
        "ProductColor" : response.css(product_schema['ProductColor']).get(),
        "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
        "OriginalPrice": response.css(product_schema['OldPrice']).get(),
        "Department": response.css(product_schema['Department']).get(),
        "info": response.css(product_schema['info']).get(),
        "NoDiscountPrice": response.css(product_schema['NoDiscountPrice']).get(),
        "ProductURL": response.url,
        }

        no_discount = product.pop("NoDiscountPrice" , None)
        if no_discount:
            product['CurrentPrice'] = no_discount
            product['OriginalPrice'] = no_discount


        product['PriceCurrency'] = product['CurrentPrice'].split(" ")[-1]

        info_data = json.loads(product.pop('info'))
        product['AvailableSizes'] = info_data['size']
        product['Category'] = info_data['category']
        product['sku'] = info_data['sku']

        product['CurrentPrice'] = float(re.sub(r'[^\d.]', '', product['CurrentPrice']))
        product['OriginalPrice'] = float(re.sub(r'[^\d.]', '', product['OriginalPrice']))

        yield product

class AnswearDonna(AnswearUomo):
    """Spider for Answear store Donna section."""
    name = "answear-donna"

class AnswearBambini(AnswearUomo):
    """Spider for Answear store Bambini section."""
    name = "answear-bambini"

