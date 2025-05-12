import re

from .base_scraper import NextPageScraper

class PellecchiaSpider(NextPageScraper):
    """Spider for Pellecchia store"""
    name = "pellecchia"
    allowed_domains = ["pellecchia.it"]
    start_urls = ["https://www.pellecchia.it/"]

    def parse_product_page(self, response):
        product_schema = self.schema['product_page_schema']
        product = {
            "Brand": response.css(product_schema['Brand']).get(),
            "ProductName": response.css(product_schema['ProductName']).get(),
            "ProductImage": response.css(product_schema['ProductImage']).get(),
            "sku": response.css(product_schema['SkuCode']).get(),
            "CurrentPrice": response.css(product_schema['CurrentPrice']).get(),
            "OriginalPrice": response.css(product_schema['OldPrice']).get(),
            "AvailableSizes": response.css(product_schema['AvailableSizes']).getall(),
            "Description": response.css(product_schema['Description']).get(),
            "ProductColor": response.css(product_schema['ProductColor']).get(),
            "Category": response.css(product_schema['Category']).get(),
        }
        product['Brand'] = self._clean_string(product['Brand'])
        product["ProductCode"] = product["sku"].replace("SKU: " , "")
        product['PriceCurrency'] = self._convert_currency_symbols_to_code(product['CurrentPrice'])
        product['OriginalPrice'] = float(re.search(r'[\d,]+', product['OriginalPrice']).group().replace(',', ''))
        product['CurrentPrice'] = float(re.search(r'[\d,]+', product['CurrentPrice']).group().replace(',', ''))
        product['AvailableSizes'] = [self._clean_string(size) for size in product['AvailableSizes']]

        yield product

    def _clean_string(self , text: str) -> str:
        """
        Clean a string by:
        - Removing all line breaks and tabs
        - Replacing multiple spaces with single space
        - Trimming leading/trailing whitespace
        - Handling None values safely

        Args:
            text (str): Input string to clean

        Returns:
            str: Cleaned string or empty string if input was None
        """
        if text is None:
            return ""

        # Replace all whitespace characters (including \n, \t, etc.) with single space
        cleaned = ' '.join(str(text).split())

        return cleaned.strip()

    def _convert_currency_symbols_to_code(self, price_string):
        """Convert currency symbol to code
        :arg
            price_string: string e.g. '€75.5', '60$', '3£'
        :return
            string: string e.g. 'EUR', 'USD', 'GBP'
        """
        currency_symbol = self._extract_currency_symbols(price_string)
        if currency_symbol == '€':
            return 'EUR'
        elif currency_symbol == '$':
            return 'USD'
        elif currency_symbol == '£':
            return 'GBP'
        else:
            return price_string

    def _extract_currency_symbols(self,text):
        # Matches common currency symbols (including crypto and rare ones)
        pattern = r'[€$£¥₹₽₩₪₺₴₸₿฿₫៛₡₱₲₵₶₾₼₠₣₤₥₦₧₨₩₪₫₭₮₯₰₳₷₺₻₼₽₾₿]'
        return re.findall(pattern, text)[0]