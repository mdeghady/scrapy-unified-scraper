import re

from .base_scraper import NextPageScraper, DataCleanser


class Wardow(NextPageScraper, DataCleanser):
    """Spider for Wardow store"""
    name = "wardow"
    allowed_domains = ["wardow.com"]
    start_urls = ["https://www.wardow.com/it"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
        "Brand": response.css(product_schema['Brand']).get(),
        "ProductName": response.css(product_schema['ProductName']).get(),
        "ProductImage": response.css(product_schema['ProductImage']).get(),
        "ProductColor" : response.css(product_schema['ProductColor']).get(),
        "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
        "OriginalPrice": response.css(product_schema['OldPrice']).get(),
        "NoDiscountPrice": response.css(product_schema['NoDiscountPrice']).get(),
        "PriceCurrency": response.css(product_schema['PriceCurrency']).get(),
        "Category": response.css(product_schema['Category']).get(),
        "sku": response.css(product_schema['sku']).get(),
        "AvailableSizes" : "One Size",
        "WebCode" : response.css(product_schema['WebCode']).getall(),
        "StockAvailability" : response.css(product_schema['StockAvailability']).get(),
        "ProductURL": response.url,
        }

        # Clean Data
        product['Brand'] = product['Brand'].replace("\n" , "").strip()
        product['Category'] = product['Brand'].replace("\n", "").strip()
        product['WebCode'] = product['WebCode'][-1].replace("\n", "").strip() if product['WebCode'] else None



        if product['NoDiscountPrice']:
            product['CurrentPrice'] = float(
            re.search(r'[\d,]+', product['NoDiscountPrice']).group().replace(',', '.'))
            product['OriginalPrice'] = product['CurrentPrice']
        else:
            product['CurrentPrice'] = float(
                re.search(r'[\d,]+', product['CurrentPrice']).group().replace(',', '.'))
            product['OriginalPrice'] = float(
                re.search(r'[\d,]+', product['OriginalPrice']).group().replace(',', '.'))


        yield product
