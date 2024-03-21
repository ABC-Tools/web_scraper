import logging
import os
from enum import Enum
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

import scrapy
# https://github.com/hhursev/recipe-scrapers
from recipe_scrapers import scrape_html
from scrapy.pipelines.images import ImagesPipeline


class UrlProtocol(Enum):
    HTTPS = 1
    FILE = 2


class CustomImagesPipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        return PurePosixPath(urlparse(request.url).path).name


class ImageItem(scrapy.Item):
    image_urls = scrapy.Field()
    images = scrapy.Field()


class FoodNetworkSpider(scrapy.Spider):
    name = 'food_network'

    def start_requests(self):
        for page_no in range(5000, 10139):
            local_page = FoodNetworkSpider.get_local_file_for_search_page(page_no)
            if os.path.isfile(local_page):
                logging.info('Search page ({}) has been crawled before; use local file: {}'.format(
                    page_no, local_page))
                yield scrapy.Request('file://{}'.format(local_page), self.parse_search)
            else:
                url = 'https://www.foodnetwork.com/search/recipe-/p/{page_no}'.format(page_no=page_no)
                logging.info('Download new search page: {} at {}'.format(page_no, url))
                yield scrapy.Request(url, self.parse_search)

    def parse_search(self, response):
        proto, page_no = FoodNetworkSpider.parse_search_url(response.url)

        if proto == UrlProtocol.HTTPS:
            local_page = FoodNetworkSpider.get_local_file_for_search_page(page_no)
            Path(local_page).write_bytes(response.body)

        links = response.css("section.o-RecipeResult.o-ResultCard h3.m-MediaBlock__a-Headline a::attr(href)").getall()
        for raw_link in links:
            # raw link is like "//www.foodnetwork.com/recipes/korean-bulgogi-taco-recipe-2346079"
            if raw_link.startswith('http'):
                link = raw_link
            else:
                link = 'https:{raw_link}'.format(raw_link=raw_link)

            _, recipe_name = FoodNetworkSpider.parse_recipe_url(link)

            if FoodNetworkSpider.is_recipe_scraped(recipe_name):
                local_filepath = 'file://{}'.format(FoodNetworkSpider.get_recipe_path(recipe_name))
                logging.info('recipie ({}) has been crawled before; use local file: {}'.format(
                    recipe_name, local_filepath))
                yield scrapy.Request(local_filepath, self.parse_recipe)
            else:
                logging.info('Find new recipe: {} at {}'.format(recipe_name, link))
                yield scrapy.Request(link, self.parse_recipe)

    def parse_recipe(self, response):
        proto, recipe_name = FoodNetworkSpider.parse_search_url(response.url)

        if proto == UrlProtocol.HTTPS:
            filename = FoodNetworkSpider.get_recipe_path(recipe_name)
            Path(filename).write_bytes(response.body)

        s = scrape_html(response.body, org_url=response.url)

        # TODO: download image: s.image()
        # yield ImageItem(image_urls=[s.image()])

        yield s.to_json()

    @staticmethod
    def is_recipe_scraped(recipe_name: str) -> bool :
        return os.path.isfile(FoodNetworkSpider.get_recipe_path(recipe_name))

    @staticmethod
    def get_recipe_path(recipe_name: str) -> str:
        return '{}/{}.html'.format(FoodNetworkSpider.get_data_dir(), recipe_name)

    @staticmethod
    def parse_recipe_url(url: str):
        if url.startswith('https://'):
            protocol = UrlProtocol.HTTPS
        else:
            protocol = UrlProtocol.FILE

        segments = url.split('/')
        # skip ending empty segment if there is one
        if not segments[-1]:
            segments.pop()

        if protocol == UrlProtocol.HTTPS:
            recipe_name = segments[-1]
        else:
            recipe_name = segments[-1].split('.')[0]

        return protocol, recipe_name

    @staticmethod
    def parse_search_url(url: str):
        """
        possible URLs
            1. remote: https://www.foodnetwork.com/search/recipe-/p/10139
            2. local: file:///Users/santan/gitspace/web_scraper/recipes/recipes/data/foodnetwork/index_10139.html
        :param url:
        :return:
        """
        if url.startswith('https://'):
            protocol = UrlProtocol.HTTPS
        else:
            protocol = UrlProtocol.FILE

        segments = url.split('/')
        # skip ending empty segment if there is one
        if not segments[-1]:
            segments.pop()

        if protocol == UrlProtocol.HTTPS:
            page_no = segments[-1]
        else:
            page_no = segments[-1].split('.')[0].split('_')[-1]

        return protocol, page_no

    @staticmethod
    def get_local_file_for_search_page(page_no):
        return '{directory}/index_{page_no}.html'.format(
            directory=FoodNetworkSpider.get_data_dir(), page_no=page_no)

    @staticmethod
    def get_data_dir():
        return os.path.join(
            os.path.dirname(  # recipes directory
                os.path.dirname(os.path.abspath(__file__))  # spiders directory
            ),
            'data', 'foodnetwork'
        )
