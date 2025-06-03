import re



from unifiedscraper.spiders.base_scraper import NextPageScraper, DataCleanser


class Durso(NextPageScraper , DataCleanser):
    """Spider for Deflorio store"""

    name = "durso"
    allowed_domains = ["dursoboutique.com"]
    start_urls = ["https://www.dursoboutique.com"]

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
            "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
            "Category": response.css(product_schema['Category']).get(),
            "Department": response.css(product_schema['Department']).get(),
            "sku": response.css(product_schema['sku']).get(),
            "ProductURL": response.url,
            "Availability" : "".join(response.css(product_schema['Availability']).getall())
        }

        # Clean the data
        product['PriceCurrency'] = self._convert_currency_symbols_to_code(product['CurrentPrice'])
        product['CurrentPrice'] = float(
            re.search(r'[\d,]+', product['CurrentPrice']).group().replace(',', '.'))
        if product['OriginalPrice']:
            product['OriginalPrice'] = float(
                re.search(r'[\d,]+', product['OriginalPrice']).group().replace(',', '.'))
        else:
            product['OriginalPrice'] = product['CurrentPrice']

        # Clean the availability
        if product['Availability']:
            product['Availability'] = product['Availability'].replace("\n" , "").strip()
        else:
            product['Availability'] = None

        if not product["ProductColor"]:
            product["ProductColor"] = product["ProductURL"].split("-")[-1]
        if 'html' in product["ProductColor"]:
            product["ProductColor"] = response.css(product_schema['alternateColor']).get()

        yield product