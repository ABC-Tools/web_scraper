"""
Crawl ratings from behindthename, like https://www.behindthename.com/name/george/rating

The download pages are stored at /Users/santan/Downloads/behindthename/rating

"""

import json
from pathlib import Path

import re
import scrapy
import os
from enum import Enum


class UrlProtocol(Enum):
    HTTPS = 1
    FILE = 2


class BehindTheNameSpider(scrapy.Spider):
    name = "name_ratings"

    def start_requests(self):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        top_names_file = '{}/top_names_2013_to_2022.json'.format(cur_dir)
        with open(top_names_file, 'r') as fp:
            top_names = json.load(fp)

        # for each name, yield a URL
        count = 0
        for name_dict in top_names:
            name = name_dict['name']

            crawled = BehindTheNameSpider.is_rating_crawled(name)
            if crawled:
                url = 'file://{}'.format(BehindTheNameSpider.get_local_file(name))
                print('Name {} has been crawled before; use local file: {}'.format(name, url))
            else:
                url = 'https://www.behindthename.com/name/{}/rating'.format(name.lower())
                print('crawl {} with url: {}'.format(name, url))

            yield scrapy.Request(url, self.parse_rating)

            count += 1
            """
            if count > 10:
                return
            """

    def parse_rating(self, response):
        proto, name, seq_num = BehindTheNameSpider.parse_url(response.url)
        name = name.lower()

        # save local file if we fetch from website
        if proto == UrlProtocol.HTTPS:
            filename = BehindTheNameSpider.get_local_file(name, seq_num=seq_num)
            Path(filename).write_bytes(response.body)

        # if there is a text like "There were no ratings found for ...", try different URL
        if response.xpath("//*[contains(text(), 'There were no ratings found for')]"):
            # get all related links
            all_links = ['https://www.behindthename.com{}'.format(x.get())
                         for x in response.css("div.browsename span.listname a::attr(href)")]
            print('name {} find new links: {}'.format(name, ', '.join(all_links)))
            for link in all_links:
                new_proto, new_name, new_seq_num = BehindTheNameSpider.parse_url(link)
                crawled = BehindTheNameSpider.is_rating_crawled(new_name, seq_num=new_seq_num)
                if crawled:
                    url = 'file://{}'.format(BehindTheNameSpider.get_local_file(new_name, seq_num=new_seq_num))
                else:
                    url = link

                print('crawl {} with url: {}'.format(name, url))
                yield scrapy.Request(url, self.parse_rating)
            return

        # Parse the ratings
        ratings = []
        for tr in response.xpath("//center//table//tr"):
            tds = tr.css("td ::text")
            left_attr = tds[0].get()
            left_rating = tds[2].get()
            right_rating = tds[3].get()
            right_attr = tds[5].get()

            ratings.append({left_attr: left_rating, right_attr: right_rating})

        voted_sentence = response.css("center p:contains('Based on the responses of')::text")[0].get()
        voted_people = re.search(r'[,\d]+', voted_sentence).group(0)
        voted_people = voted_people.replace(',', '')
        yield {
            'name': name,
            'rating': ratings,
            'voted_people': voted_people
        }

    @staticmethod
    def is_rating_crawled(name, seq_num=None):
        return os.path.isfile(
            BehindTheNameSpider.get_local_file(name, seq_num))

    @staticmethod
    def get_local_file(name, seq_num=None):
        name = name.lower()
        directory = '/Users/santan/Downloads/behindthename/rating'
        if seq_num:
            return '{directory}/{name}-{seq_num}.html'.format(directory=directory, name=name, seq_num=seq_num)
        else:
            return '{directory}/{name}.html'.format(directory=directory, name=name)

    @staticmethod
    def parse_url(url):
        '''
        possible URLs
            1. remote: https://www.behindthename.com/name/william/rating, or,
                       https://www.behindthename.com/name/william-1/rating
            2. local: file:///Users/santan/Downloads/behindthename/rating/william.html
        :param url:
        :return:
        '''
        if url.startswith('https://'):
            protocol = UrlProtocol.HTTPS
        else:
            protocol = UrlProtocol.FILE

        segments = url.split('/')
        # skip ending empty segment if there is one
        if not segments[-1]:
            segments.pop()

        seq_num = None
        if protocol == UrlProtocol.HTTPS:
            result_list = segments[-2].split('-')
            name = result_list[0]
            if len(result_list) > 1:
                seq_num = int(result_list[1])
        else:
            name = segments[-1].split('.')[0]

        return protocol, name, seq_num
