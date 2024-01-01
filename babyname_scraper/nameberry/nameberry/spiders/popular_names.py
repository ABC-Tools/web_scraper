from pathlib import Path

import logging
import scrapy
import os
from enum import Enum
import json


class Gender(Enum):
    GIRL = 1
    BOY = 2

    def __str__(self):
        return f'{self.name}'.lower()


class UrlProtocol(Enum):
    HTTPS = 1
    FILE = 2


class OutputContent(Enum):
    NAME_MEANING = 1
    SIMILAR_NAMES = 2


output_content = OutputContent.NAME_MEANING
PARSE_LOCAL_ONLY = True


class NameBerrySpider(scrapy.Spider):
    name = "popular_names"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        cur_dir = os.path.dirname(os.path.abspath(__file__))
        top_names_file = '{}/top_names_2013_to_2022.json'.format(cur_dir)
        with open(top_names_file, 'r') as fp:
            top_names = json.load(fp)

        self._top_names = []
        for name_dict in top_names:
            self._top_names.append(name_dict['name'].lower())
        self._top_names_set = set(self._top_names)

    def start_requests(self):
        """
        urls = [
            # the index page is rendered by Javascript, which is not supported by Scrapy
            'file:///Users/santan/Downloads/Most Popular Baby Names 2022 _ Nameberry.html',
            'file:///Users/santan/Downloads/Popular Girl Names 2023 _ Nameberry.html',
            'file:///Users/santan/Downloads/Popular Boy Names 2023 _ Nameberry.html'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_popular_names)
        """
        # for each name, yield a URL
        count = 0
        for name in self._top_names:
            crawled = NameBerrySpider.is_name_crawled(name)
            if crawled:
                url = 'file://{}'.format(NameBerrySpider.get_local_file_name(name))
                logging.info('Name {} has been crawled before; use local file: {}'.format(name, url))
                self.crawler.stats.inc_value('user/local_file_hit')
                yield scrapy.Request(url, self.parse_name_page)
            else:
                url = 'https://babynames.com/name/{}'.format(name.lower())
                logging.info('crawl {} with url: {}'.format(name, url))
                self.crawler.stats.inc_value('user/remote_url_load')
                if not PARSE_LOCAL_ONLY:
                    yield scrapy.Request(url, self.parse_name_page)

            count += 1
            # if count >= 10:
            #  break

    def parse_name_page(self, response):
        proto, name, gender = NameBerrySpider.parse_url(response.url)
        logging.info('user: {url}, {name}, {gender}'.format(url=response.url, name=name, gender=gender))

        # Save to file if it is just downloaded
        if proto == UrlProtocol.HTTPS:
            self.crawler.stats.inc_value('user/https_success')
            filename = NameBerrySpider.get_local_file_name(name, gender)
            Path(filename).write_bytes(response.body)

        # if the name can be used by both boys and girls, the page is an anchor page;
        # we need to load the gender specific page
        val = "{} Continued".format(name.capitalize())
        if len(response.xpath("//a[contains(., $val)]/@href", val=val)) > 0:
            if NameBerrySpider.is_name_crawled(name, Gender.GIRL):
                link = 'file://{}'.format(NameBerrySpider.get_local_file_name(name, Gender.GIRL))
                logging.info('cached for name {} and gender {}'.format(name, Gender.GIRL))
                yield scrapy.Request(link, self.parse_name_page)
            else:
                link = NameBerrySpider.get_url(name, Gender.GIRL)
                logging.info('Crawl remote for name {} and gender {}'.format(name, Gender.GIRL))
                if not PARSE_LOCAL_ONLY:
                    yield scrapy.Request(link, self.parse_name_page)

            if NameBerrySpider.is_name_crawled(name, Gender.BOY):
                link = 'file://{}'.format(NameBerrySpider.get_local_file_name(name, Gender.BOY))
                logging.info('cached for name {} and gender {}'.format(name, Gender.BOY))
                yield scrapy.Request(link, self.parse_name_page)
            else:
                link = NameBerrySpider.get_url(name, Gender.BOY)
                logging.info('Crawl remote for name {} and gender {}'.format(name, Gender.BOY))
                if not PARSE_LOCAL_ONLY:
                    yield scrapy.Request(link, self.parse_name_page)

        # parse text
        if output_content == OutputContent.NAME_MEANING:
            description_text_str = response.xpath("string(//div[@class = 't-copy'])").get()
            if description_text_str:
                yield {
                    name: {
                        'gender': str(gender) if gender else '',
                        'description': description_text_str
                    }
                }
        elif output_content == OutputContent.SIMILAR_NAMES:
            xpath = "//li[contains(@class, 'Listing-name')]//a[contains(@href, '/babyname/')]/@href"
            similar_name_links = response.xpath(xpath).getall()
            similar_names = [x.split('/')[-1] for x in similar_name_links]

            filtered_similar_names = []
            for sname in similar_names:
                if sname in self._top_names_set:
                    filtered_similar_names.append(sname)
            if not filtered_similar_names:
                return

            yield_result = {
                'name': name,
                'similar_names': filtered_similar_names
            }
            if gender:
                yield_result['gender'] = str(gender)
            yield yield_result
        else:
            raise ValueError('Unexpected output content')

    @staticmethod
    def get_local_file_name(name, gender=None):
        name = name.lower()
        directory = '/Users/santan/Downloads/nameberry/'
        if gender:
            return '{}{}-{}.html'.format(directory, name, gender)
        else:
            return '{}{}.html'.format(directory, name)

    @staticmethod
    def is_name_crawled(name, gender=None):
        filename = NameBerrySpider.get_local_file_name(name, gender=gender)
        return os.path.isfile(filename)

    @staticmethod
    def get_url(name, gender=None):
        name = name.capitalize()
        if gender:
            return 'https://nameberry.com/babyname/{}/{}'.format(name, str(gender))
        else:
            return 'https://nameberry.com/babyname/{}'.format(name)

    @staticmethod
    def is_url_crawled(url):
        _, name, gender = NameBerrySpider.parse_url(url)
        return NameBerrySpider.is_name_crawled(name, gender)

    @staticmethod
    def get_name_from_url(url):
        _, name, _ = NameBerrySpider.parse_url(url)
        return name

    @staticmethod
    def parse_url(url):
        '''
        possible URLs
            1. with gender: https://nameberry.com/babyname/William/boy
            2. without gender: https://nameberry.com/babyname/William
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

        if protocol == UrlProtocol.HTTPS:
            # remove gender segment if there one
            last_seg = segments[-1]
            if 'boy' in last_seg:
                gender = Gender.BOY
                segments.pop()
            elif 'girl' in last_seg:
                gender = Gender.GIRL
                segments.pop()

            name = segments[-1]
        else:
            name = segments[-1]
            if name.endswith('.html'):
                name = name[0:-5]

            if name.endswith('-boy'):
                name = name[0:-4]
                gender = Gender.BOY
            elif name.endswith('-girl'):
                name = name[0:-5]
                gender = Gender.GIRL

        return protocol, name, gender

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
            if NameBerrySpider.is_url_crawled(url):
                name = NameBerrySpider.get_name_from_url(url)
                link = 'file://' + NameBerrySpider.get_local_file_name(name)

                print('Downloaded; Parse {}'.format(link))
                yield scrapy.Request(link, self.parse_name_page)
            else:
                print('Crawl & Parse {}'.format(link))
                yield scrapy.Request(link, self.parse_name_page)

            # test
            # if count > 5:
            #    break

        logging.info('Total Links: {}'.format(count))
