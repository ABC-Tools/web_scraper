import unittest

from news_scraper.news_scraper.spiders.bbc_chinese import QuotesSpider


class MyTestCase(unittest.TestCase):

    @staticmethod
    def test_get_storage_path():
        assert QuotesSpider.get_storage_path('https://www.bbc.com/zhongwen/simp/world-67325336') \
                == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.html"
        assert QuotesSpider.get_storage_path('https://www.bbc.com/zhongwen/simp/world-67325336.html') \
               == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.html"
        assert QuotesSpider.get_storage_path('https://www.bbc.com/zhongwen/simp/world-67325336.xhtml') \
               == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.xhtml"
        assert QuotesSpider.get_storage_path('/www.bbc.com/zhongwen/simp/world-67325336.xhtml') \
               == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.xhtml"

        assert QuotesSpider.get_storage_path('http://www.bbc.com/zhongwen/simp/world-67325336') \
               == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.html"
        assert QuotesSpider.get_storage_path('http://www.bbc.com/zhongwen/simp/world-67325336.html') \
               == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.html"
        assert QuotesSpider.get_storage_path('http://www.bbc.com/zhongwen/simp/world-67325336.xhtml') \
               == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.xhtml"
        assert QuotesSpider.get_storage_path('/www.bbc.com/zhongwen/simp/world-67325336.xhtml') \
               == "/tmp/www.bbc.com/zhongwen/simp/world-67325336.xhtml"


if __name__ == '__main__':
    unittest.main()
