import re

from unifiedscraper.spiders.base_scraper import NextPageScraper, DataCleanser


class Viglie(NextPageScraper , DataCleanser):
    """Spider for Euroshoesroma store"""

    name = "viglie"
    allowed_domains = ["vigliettisport.com"]
    start_urls = ["https://www.vigliettisport.com/it"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
            "Brand": response.css(product_schema['Brand']).get(),
            "ProductName": response.css(product_schema['ProductName']).get(),
            "ProductImage": response.css(product_schema['ProductImage']).get(),
            "ProductColor": response.css(product_schema['ProductColor']).get(),
            "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
            "OriginalPrice": response.css(product_schema['OldPrice']).get(),
            "PriceCurrency": response.css(product_schema['PriceCurrency']).get(),
            "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
            "Category": response.css(product_schema['Category']).get(),
            "sku": response.css(product_schema['sku']).get(),
            "Department": response.css(product_schema['Department']).get(), 
            "ProductURL": response.url,
            "StockAvailability" : response.css(product_schema['StockAvailability']).get()
        }
        # Clean the data
        product['Brand'] = product['Brand'].replace("\n" , "").strip()
        product['ProductName'] = product['ProductName'].replace("\n" , "").strip()
        product['StockAvailability'] = product['StockAvailability'].replace("\n" , "").strip()

        product["ProductCode"] = product['sku']

        product['CurrentPrice'] = float(
            re.search(r'[\d.]+', product['CurrentPrice']).group())
        
        if product['OriginalPrice']:
            product['OriginalPrice'] = float(
                re.search(r'[\d.]+', product['OriginalPrice']).group())
        else:
            product['OriginalPrice'] = product['CurrentPrice']

        yield product
