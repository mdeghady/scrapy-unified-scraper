from .base_scraper import LoadMoreScrapper


class LelefantinoSpider(LoadMoreScrapper):
    """Spider for Lelefantino store"""
    name = "lelefantino"
    allowed_domains = ["lelefantino-store.com"]
    start_urls = ["https://lelefantino-store.com"]

    def parse_product_page(self, response):
        """Parse the product details from the page using the schema."""
        product_schema = self.schema['product_page_schema']
        product_details = {}

        # Loop through all fields in the schema
        for field, selector in product_schema.items():
            # Check if the field requires extracting a list or an attribute
            if '::attr' in selector:
                # Extract attributes like 'href' or 'srcset'
                product_details[field] = response.css(selector).get()
            elif selector.endswith(' li'):
                # Extract list items
                product_details[field] = self.get_list_field(response, selector)
            else:
                # Extract text for standard fields
                product_details[field] = response.css(selector).get()

        # Yield the product details dictionary
        yield product_details

    def get_list_field(self, response, selector):
        """Helper function to extract list-based fields (e.g., AvailableSizes or OtherColorsURLs)."""
        items = []
        elements = response.css(selector)

        for element in elements:
            # Extract the text of the list item (or attribute if needed)
            item = element.css('span::text').get()  # Adjust the selector if needed
            items.append(item)

        return items
