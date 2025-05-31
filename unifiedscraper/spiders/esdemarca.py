import re

from .base_scraper import NextPageScraper, DataCleanser


class Esdemarca(NextPageScraper, DataCleanser):
    """Spider for Esdemarca store"""
    name = "esdemarca"
    allowed_domains = ["www.esdemarca.com"]
    start_urls = ["https://www.esdemarca.com/en/"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
        "Brand": response.css(product_schema['Brand']).get(),
        "ProductName": response.css(product_schema['ProductName']).get(),
        "ProductImage": response.css(product_schema['ProductImage']).get(),
        "ProductColor" : response.css(product_schema['ProductColor']).get(),
        "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
        "OriginalPrice": response.css(product_schema['OldPrice']).get(),
        "NoDiscountPrice" : response.css(product_schema['NoDiscountPrice']).get(),
        "Category": response.css(product_schema['Category']).get(),
        "sku": response.css(product_schema['sku']).get(),
        "AvailableSizes" : response.css(product_schema['AvailableSizes']).getall(),
        "ProductURL": response.url,
        }

        # Clean the product data
        no_discount_price = product.pop("NoDiscountPrice")
        if no_discount_price:
            product["CurrentPrice"] = no_discount_price
            product["OriginalPrice"] = no_discount_price

        product['PriceCurrency'] = self._convert_currency_symbols_to_code(product['CurrentPrice'])
        product['CurrentPrice'] = float(
            re.search(r'[\d.]+', product['CurrentPrice']).group())
        product['OriginalPrice'] = float(
            re.search(r'[\d.]+', product['OriginalPrice']).group())

        product['sku'] = product['sku'].replace("\n" , "")\
                        .replace("\t", "").split(":")[-1].strip() if product['sku'] else None

        product['ProductColor'] = product['ProductColor'].replace("\n" , "")\
                        .replace("\t", "").split(":")[-1].strip() if product['ProductColor'] else None

        yield product
