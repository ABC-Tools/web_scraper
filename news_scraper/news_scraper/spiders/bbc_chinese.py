from pathlib import Path

import scrapy
import random

HTTPS_PREFIX = "https://"
HTTP_PREFIX = "http://"


class QuotesSpider(scrapy.Spider):
    name = "bbc_chinese"
    storage_prefix = "/tmp"

    def start_requests(self):
        urls = [
            "https://www.bbc.com/zhongwen/simp/world-67325336"
        ]
        for url in urls:
            self.assert_url_prefix(url)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.assert_url_prefix(response.url)

        filename = QuotesSpider.get_storage_path(response.url)
        Path(filename).write_bytes(response.body)

        self.log(f"Saved file {filename}")

    @staticmethod
    def assert_url_prefix(url):
        assert url.lower().startswith(QuotesSpider.path_prefix.lower())

    @staticmethod
    def get_storage_path(url):
        if url.lower().startswith(HTTPS_PREFIX):
            rel_path = url[len(HTTPS_PREFIX):]
        elif url.lower().startswith(HTTP_PREFIX):
            rel_path = url[len(HTTP_PREFIX):]
        else:
            rel_path = url

        starting_with_slash = rel_path.startswith("/")
        file_path = QuotesSpider.storage_prefix + (f"" if starting_with_slash else f"/") + rel_path

        if not rel_path.endswith(".html") and not rel_path.endswith(".xhtml"):
            file_path = file_path + f".html"

        return file_path
