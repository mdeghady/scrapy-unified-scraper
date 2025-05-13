import re

from .base_scraper import NextPageScraper, DataCleanser


class GRS(NextPageScraper,DataCleanser):
    """Spider for GRS store"""
    name = "grs"
    allowed_domains = ["grsboutique.com"]
    start_urls = ["https://grsboutique.com/"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {"Brand": response.css(product_schema['Brand']).get(),
                   "ProductName": response.css(product_schema['ProductName']).get(),
                   "ProductImage": response.css(product_schema['ProductImage']).get(),
                   "CurrentPrice": response.css(product_schema['CurrentPrice']).getall()[0],
                   "OriginalPrice": response.css(product_schema['OldPrice']).getall()[0],
                   "AvailableSizes": response.css(product_schema['AvailableSizes']).getall()}

        product["Category"] = product["ProductName"].split(" ")[0]
        product["ProductLink"] = response.url
        product["sku"] = response.url.split("/")[-1]
        sku_splitted = product["sku"].split("-")
        product["ProductCode"] = sku_splitted[0]

        if len(sku_splitted) > 1:
            product["Color"] = sku_splitted[1]
        else:
            product["Color"] = ""

        product['ProductCurrency'] = self._convert_currency_symbols_to_code(response)
        product['OriginalPrice'] = float(
            re.search(r'[\d,]+', product['OriginalPrice'].replace(",", "")).group().replace(",", "."))
        product['CurrentPrice'] = float(
            re.search(r'[\d,]+', product['CurrentPrice'].replace(",", "")).group().replace(",", "."))

        yield product