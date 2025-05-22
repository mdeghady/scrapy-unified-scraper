import re



from unifiedscraper.spiders.base_scraper import NextPageScraper, DataCleanser


class Itgio(NextPageScraper , DataCleanser):
    """Spider for Deflorio store"""

    name = "itgio"
    allowed_domains = ["itgiocollection.com"]
    start_urls = ["https://www.itgiocollection.com/"]

    def parse_product_page(self, response):
        """Parse product page and extract product data"""
        product_schema = self.schema['product_page_schema']
        product = {
            "Brand": response.css(product_schema['Brand']).get(),
            "ProductName": response.css(product_schema['ProductName']).get(),
            "ProductImage": response.css(product_schema['ProductImage']).get(),
            "ProductColor": response.css(product_schema['ProductColor']).get(),
            "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
            "OriginalPrice": response.css(product_schema['OldPrice']).get(),
            "PriceCurrency" : response.css(product_schema['PriceCurrency']).get(),
            "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
            "Category": response.css(product_schema['Category']).get(),
            "sku": response.css(product_schema['sku']).get(),
            "ProductURL": response.url,
            "Availability" : "".join(response.css(product_schema['Availability']).getall())
        }

        #Clean Data
        try:
            product['OriginalPrice'] = float(
                re.search(r'[\d,]+', product['OriginalPrice']).group().replace(',', '.'))
            product['CurrentPrice'] = float(
                re.search(r'[\d,]+', product['CurrentPrice']).group().replace(',', '.'))
        except:
            product['OriginalPrice'] , product['CurrentPrice'] = None , None

        if product["ProductColor"]:
            product["ProductColor"] = product["ProductColor"].replace("Colore","").strip()

        product['Availability'] = product['Availability'].strip()

        yield product