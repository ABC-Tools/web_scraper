from pathlib import Path

import scrapy
import os
from enum import Enum


class Gender(Enum):
    GIRL = 1
    BOY = 2

    def __str__(self):
        return f'{self.name}'.lower()

class UrlProtocol(Enum):
    HTTPS = 1
    FILE = 2


class QuotesSpider(scrapy.Spider):
    name = "popular_names"

    def start_requests(self):
        urls = [
            # the index page is rendered by Javascript, which is not supported by Scrapy
            'file:///Users/santan/Downloads/Most Popular Baby Names 2022 _ Nameberry.html',
            'file:///Users/santan/Downloads/Popular Girl Names 2023 _ Nameberry.html',
            'file:///Users/santan/Downloads/Popular Boy Names 2023 _ Nameberry.html'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_popular_names)

    def parse_popular_names(self, response):
        count = 0
        for link_obj in response.xpath("//ul/li/a/@href"):
            link = link_obj.get()
            if link.startswith('https://nameberry.com/babyname/'):
                url = link
            elif link.startswith('/babyname/'):
                url = 'https://nameberry.com' + link
            else:
                continue

            count += 1
            if QuotesSpider.is_name_crawled(url):
                name = QuotesSpider.get_name_from_url(url)
                link = 'file://' + QuotesSpider.get_local_file_name(name)

                print('Downloaded; Parse {}'.format(link))
                yield scrapy.Request(link, self.parse_name_page)
            else:
                print('Crawl & Parse {}'.format(link))
                yield scrapy.Request(link, self.parse_name_page)

            # test
            # if count > 5:
            #    break

        print('Total Links: {}'.format(count))

    def parse_name_page(self, response):
        proto, name, gender = QuotesSpider.parse_url(response.url)

        # Save to file if it is just downloaded
        if proto == UrlProtocol.HTTPS:
            filename = QuotesSpider.get_local_file_name(name, gender)
            Path(filename).write_bytes(response.body)

        print('Parsing for {} with url of {}'.format(name, response.url))

        # if the name can be used by both boys and girls, the page is an anchor page;
        # we need to load the gender specific page
        val = "{} Continued".format(name.capitalize())
        if len(response.xpath("//a[contains(., $val)]/@href", val=val)) > 0:
            link = QuotesSpider.get_url(name, Gender.GIRL)
            print('Crawl & Parse {}'.format(link))
            yield scrapy.Request(link, self.parse_name_page)

            link = QuotesSpider.get_url(name, Gender.BOY)
            print('Crawl & Parse {}'.format(link))
            yield scrapy.Request(link, self.parse_name_page)

            return

        # parse text
        description_text_list = response.xpath("//div[@class = 't-copy']//text()").getall()
        if len(description_text_list) > 0:
            yield {
                name: {
                    'gender': str(gender),
                    'description': description_text_list
                }
            }

    @staticmethod
    def get_local_file_name(name, gender=None):
        directory = '/Users/santan/Downloads/nameberry/'
        if gender:
            return '{}{}-{}.html'.format(directory, name, gender)
        else:
            return '{}{}.html'.format(directory, name)

    @staticmethod
    def get_url(name, gender=None):
        if gender:
            return 'https://nameberry.com/babyname/{}/{}'.format(name, str(gender))
        else:
            return 'https://nameberry.com/babyname/{}'.format(name)

    @staticmethod
    def is_name_crawled(url):
        _, name, gender = QuotesSpider.parse_url(url)
        filename = QuotesSpider.get_local_file_name(name, gender)
        return os.path.isfile(filename)

    @staticmethod
    def get_name_from_url(url):
        _, name, _ = QuotesSpider.parse_url(url)
        return name

    @staticmethod
    def parse_url(url):
        '''
        possible URLs
            1. with gender: https://nameberry.com/babyname/william/boy
            2. without gender: https://nameberry.com/babyname
            3. local file with gender: file:///Users/santan/Downloads/nameberry/liam-girl.html
            4. local file without gender: file:///Users/santan/Downloads/nameberry/liam.html
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

        # initialize gender
        gender = None

        # remove gender segment if there one
        last_seg = segments[-1]
        if 'boy' in last_seg:
            gender = Gender.BOY
            segments.pop()
        elif 'girl' in last_seg:
            gender = Gender.GIRL
            segments.pop()

        # remove
        name = segments[-1]
        if name.endswith('.html'):
            name = name[0:-5]
        if name.endswith('-boy)'):
            name = name[0:-4]
            gender = Gender.BOY
        elif name.endswith('-girl)'):
            name = name[0:-5]
            gender = Gender.GIRL

        return protocol, name, gender



