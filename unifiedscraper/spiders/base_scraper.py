import json
import re
from abc import ABC , abstractmethod
from typing import AsyncIterator, Any, List

import scrapy
from pathlib import Path
from urllib.parse import urljoin


class BaseScraper(scrapy.Spider , ABC):
    name = None
    allowed_domains = None
    start_urls = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = self._load_config()
        self.schema = self._load_schema()

    def _load_config(self):
        # Load the configuration file
        config_path = Path(__file__).parent.parent / 'configs' / 'websites.json'
        with open(config_path, 'r') as file:
            config = json.load(file)

        return config[self.name]

    def _load_schema(self):
        # Load the schema file
        schema_path = Path(__file__).parent.parent / self.config['schema_path']
        with open(schema_path, 'r') as file:
            schema = json.load(file)

        return schema

    def make_absolute_url(self, follow_url, parent_url = None):
        """Convert any URL to absolute form"""
        if parent_url is None:
            parent_url = self.config['base_url']

        abs_url = urljoin(parent_url, follow_url)
        return abs_url  # Handle relative paths



    async def start(self) -> AsyncIterator[Any]:
        """Start the scraper"""
        # Initialize the spider to start scraping from brands page
        brands_page = self.make_absolute_url(self.config['brands_url'])
        self.logger.info(f"Starting scraper for {self.name} at {brands_page}")

        yield scrapy.Request(brands_page,
                             callback=self.parse_site_brand_page,
                             meta={
                                 "playwright": self.config.get('playwright' , False),
                             })

    def parse_site_brand_page(self, response):
        """Parse the brand page"""
        # Extract brand URLs from the response
        brand_urls = response.css(self.schema['brands_urls_schema']).getall()
        self.logger.info(f"Trying to extract brand URLs with selector: {self.schema['brands_urls_schema']} with {response.url}")
        self.logger.info(f"Found {len(brand_urls)} brand URLs")
        yield from self.parse_urls(response,
                        brand_urls,
                        self.parse_site_products_page,
                        meta={'brand_url': response.url})

    def parse_urls(self,response ,urls:List[str] ,parsing_function ,meta:dict = None):
        """Follow any url URLs"""
        self.logger.info(f"starting to parse urls like {urls[0]} with {parsing_function.__name__}")
        self.logger.info(f"Processing URLs: {urls}")
        for url in urls:

            absolute_url = self.make_absolute_url(url)
            if self.allowed_domains[0] in absolute_url:
                yield response.follow(absolute_url,
                                     callback=parsing_function,
                                     meta=meta)
            else:
                self.logger.debug(f"Skipping external URL: {absolute_url}")

    @abstractmethod
    def parse_site_products_page(self, response):
        pass

    @abstractmethod
    def parse_product_page(self, response):
        """Parse the product page"""
        pass

class LoadMoreScrapper(BaseScraper):
    """Scraper for websites that use a "Load More" button to load products"""
    def parse_site_products_page(self, response):
        """Parse the products page"""
        # Extract product URLs from the response
        cur_page_products_urls = response.css(self.schema['products_urls_schema']).getall()
        self.logger.info(f"Found {len(cur_page_products_urls)} product URLs on page {response.url}")
        self.parse_urls(response,
                        cur_page_products_urls,
                        self.parse_product_page,
                        meta={'brand_url': response.meta['brand_url'],
                              "playwright": self.config.get('playwright' , False),})

        # Check if there is a "Load More" button
        load_more_button = response.css(self.schema['pagination_schema'])
        if load_more_button:
            # Update the offset for the next request
            if self.config['load_more_offset'] in response.url:
                cur_offset = response.meta.get('last_offset', 0) + self.config['load_more_offset']
            else:
                # If the offset is not in the URL, set the offset to get the second page
                cur_offset = self.config['load_more_offset'] * 2

            # If there is a "Load More" button, follow the URL to load more products
            load_more_url = self.make_absolute_url(
                f"{self.config['load_more_query']}{cur_offset}",
                parent_url=response.url
            )
            yield response.follow(load_more_url,
                                 callback=self.parse_site_products_page,
                                 meta={'brand_url': response.meta['brand_url'],
                                       'last_offset': cur_offset})

class NextPageScraper(BaseScraper):
    def parse_site_products_page(self, response):
        """Parse the products page"""
        # Extract product URLs from the response
        cur_page_products_urls = response.css(self.schema['products_urls_schema']).getall()
        self.logger.info(f"Found {len(cur_page_products_urls)} product URLs on page {response.url}")
        yield from self.parse_urls(response,
                        cur_page_products_urls,
                        self.parse_product_page,
                        meta={'brand_url': response.meta['brand_url'],
                              "playwright": self.config.get('playwright' , False),})

        # Check if there is a "Next Page" button
        next_page_button = response.css(self.schema['pagination_schema']).get()
        if next_page_button:
            # Follow the URL to the next page
            next_page_url = self.make_absolute_url(next_page_button)
            yield response.follow(next_page_url,
                                 callback=self.parse_site_products_page,
                                 meta={'brand_url': response.meta['brand_url']})


class DataCleanser():
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