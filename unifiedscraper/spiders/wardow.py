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
        product['WebCode'] = str(product['WebCode']) if product['WebCode'] else None



        if product['NoDiscountPrice']:
            product['CurrentPrice'] = float(
            re.search(r'[\d.,]+', product['NoDiscountPrice']).group().replace('.' , "").replace(',', '.'))
            product['OriginalPrice'] = product['CurrentPrice']
        else:
            product['CurrentPrice'] = float(
                re.search(r'[\d.,]+', product['CurrentPrice']).group().replace('.' , "").replace(',', '.'))
            product['OriginalPrice'] = float(
                re.search(r'[\d.,]+', product['OriginalPrice']).group().replace('.' , "").replace(',', '.'))
        product.pop('NoDiscountPrice')
        description = {}
        
        # Extract key-value pairs from description-general
        for li in response.css('div.description-general li'):
            key = li.css('strong::text').get()
            if key:
                key = key.strip(':').strip()
                value = li.xpath('text()').getall()
                value = ' '.join(v.strip() for v in value if v.strip())
                description[key] = value
    
        
        # Extract product details (list items)
        description['Product Details'] = response.css('div.description-details li::text').getall()
        description['Product Details'] = [detail.strip() for detail in description['Product Details'] if detail.strip()]
        
        # Extract interior details
        description['Interior'] = response.css('div.description-inside li::text').getall()
        description['Interior'] = [detail.strip() for detail in description['Interior'] if detail.strip()]

        product['Description'] = description

        yield product
