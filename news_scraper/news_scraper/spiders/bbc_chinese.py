from pathlib import Path

import scrapy
import random

class QuotesSpider(scrapy.Spider):
    name = "bbc_chinese"

    def start_requests(self):
        urls = [
            "https://www.bbc.com/zhongwen/simp/world-67325336"
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page_path = response.url.split("/")[-1]
        page = page_path if page_path else random.randint()

        filename = f"bbc-chinese-{page}.html"
        Path(filename).write_bytes(response.body)
        self.log(f"Saved file {filename}")