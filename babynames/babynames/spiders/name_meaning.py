"""
Crawl meaning from babynames.com

The download pages are stored at /Users/santan/Downloads/babynames/downloaded
The final outcome is at /Users/santan/Downloads/babynames/babynames_meaning.json, which covers 11186 names


To avoid being blocked by GoDaddy firewall,
1. Adjust CONCURRENT_REQUESTS_PER_DOMAIN = 1
2. follow https://github.com/scrapy-plugins/scrapy-splash to enable Javascript in Scrapy

"""

import json
from pathlib import Path
import logging

from typing import List, Union

import scrapy
import os
from enum import Enum

from scrapy_splash import SplashRequest


class UrlProtocol(Enum):
    HTTPS = 1
    FILE = 2


PARSE_LOCAL_ONLY = True


class BabyNamesSpider(scrapy.Spider):
    name = "name_meaning"

    def start_requests(self):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        top_names_file = '{}/top_names_2013_to_2022.json'.format(cur_dir)
        with open(top_names_file, 'r') as fp:
            top_names = json.load(fp)

        # for each name, yield a URL
        count = 0
        for name_dict in top_names:
            name = name_dict['name']

            crawled = BabyNamesSpider.is_rating_crawled(name)
            if crawled:
                url = 'file://{}'.format(BabyNamesSpider.get_local_file(name))
                logging.info('Name {} has been crawled before; use local file: {}'.format(name, url))
                self.crawler.stats.inc_value('user/local_file_hit')
                yield scrapy.Request(url, self.parse_meaning)
            else:
                url = 'https://babynames.com/name/{}'.format(name.lower())
                logging.info('crawl {} with url: {}'.format(name, url))
                self.crawler.stats.inc_value('user/remote_url_load')
                if not PARSE_LOCAL_ONLY:
                    yield SplashRequest(url, self.parse_meaning)

            count += 1
            # if count >= 49214:
            #   return

    def parse_meaning(self, response):
        proto, name = BabyNamesSpider.parse_url(response.url)
        name = name.lower()

        # save local file if we fetch from website
        if proto == UrlProtocol.HTTPS:
            filename = BabyNamesSpider.get_local_file(name)
            Path(filename).write_bytes(response.body)

        if response.xpath("//h1[contains(., 'No names found')]"):
            logging.info('name {} has no record'.format(name))
            self.crawler.stats.inc_value('user/no_record')
            return

        # Parse the meaning
        gender = response.css("ul.namemeta li:contains('Gender: ') a::text")[0].get()
        origin = response.css("ul.namemeta li:contains('Origin: ') a::text")[0].get()
        short_meaning = response.css("ul.namemeta li:contains('Meaning: ') ::text")[0].get()
        short_meaning = short_meaning.replace('Meaning:', '').strip()

        result_elems = response.xpath("//div[@class = 'stats']/*[not(name()='h2') and "
                                      "preceding-sibling::h2[contains(., 'What is the meaning')] and "
                                      "count(preceding-sibling::h2) = 1]")
        long_meaning_list = [elem.css("*::text").getall() for elem in result_elems]
        long_meaning = BabyNamesSpider.join_string_list_recur(long_meaning_list)

        yield {
            'name': name,
            'gender': gender,
            'origin': origin,
            'short_meaning': short_meaning,
            'long_meaning': long_meaning
        }
        self.crawler.stats.inc_value('user/yield_records')

    @staticmethod
    def join_string_list_recur(str_list: List[Union[str, List]], level=0):
        if level > 3:
            return

        new_str_list = []
        for elem in str_list:
            if isinstance(elem, list):
                new_str_list.append(BabyNamesSpider.join_string_list_recur(elem, level + 1))
            else:
                new_str_list.append(elem)

        if level == 0:
            return ' '.join(new_str_list)
        else:
            return ''.join(new_str_list)

    @staticmethod
    def is_rating_crawled(name):
        return os.path.isfile(
            BabyNamesSpider.get_local_file(name))

    @staticmethod
    def get_local_file(name):
        name = name.lower()
        directory = '/Users/santan/Downloads/babynames/downloaded'
        return '{directory}/{name}.html'.format(directory=directory, name=name)

    @staticmethod
    def parse_url(url):
        """
        possible URLs
            1. remote: https://babynames.com/name/william
            2. local: file:///Users/santan/Downloads/babynames/downloaded/william.html
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
            name = segments[-1]
        else:
            name = segments[-1].split('.')[0]

        return protocol, name
