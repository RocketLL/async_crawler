# 2018.10.01 Asynchronous Web Crawler by rocketll
# Writes crawled results to crawler.csv in working directory
# Written and tested in Python 3.7

import asyncio  # Async
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
import csv

class Crawler:
    
    def __init__(self, seed, depth, maxi:100):
        self.seed = seed
        self.depth = depth
        self.semaphore = asyncio.BoundedSemaphore(maxi)  # Maximum concurrent HTTP clients
        self.found_urls = set()  # Already found URLs
        self.session = aiohttp.ClientSession()

    async def _get(self, url):
        """HTTP GETs URL. Returns HTML if 200, prints error if failed.
        :param url: URL to HTTP GET
        :returns: raw HTML if HTTP 200 
        """
        async with self.semaphore:
            try:
                async with self.session.get(url, timeout = 10) as r:
                    raw_html = await r.read()
                    return raw_html
            except Exception as e:
                print(f"Could not HTTP GET {url}: {e}")

    def _robot_parser(self, txt, url):
        """Parses robots.txt with user-agent="*".
        :param txt: robots.txt to parse
        :param url: URL to check
        :returns: if url is allowed in robots.txt
        :rtype: bool
        """
        parser = RobotFileParser()
        if txt:
            parser.parse(txt.decode("ascii", "replace").splitlines())
            return parser.can_fetch("*", url)
        else:
            return True

    def _souper(self, raw_html):
        """Soups raw HTMLs page using bs4.
        :param raw_html: raw HTML
        :returns: bs4 HTML
        """
        if raw_html:
            souped_html = BeautifulSoup(raw_html, "html.parser")
            return souped_html

    def _link_normalize(self, raw_link, url):
        """Normalizes relative links into absolute ones.
        :param raw_link: raw link to process
        :param url: absolute URL to join into
        :returns: absolute URL
        """
        if raw_link:
            abs_url = urljoin(url, raw_link)
            return abs_url
    
    def _scan_links(self, souped_html, url):
        """scans links in a bs4 process HTML page. Finds all <a> href in HTML.
        :param souped_html: bs4 souped HTML
        :param url: URL to normalize absolute links into
        :returns: scanned urls
        :rtype: list
        """
        scanned_urls = []
        for rawlink in souped_html.find_all("a"):  # <a> tag
            link = rawlink.get("href")  # href
            abs_url = self._link_normalize(link, url)
            if abs_url:
                scanned_urls.append(abs_url)
        return scanned_urls
    
    async def _find_data(self, url):
        """Finds links, titles and descriptions from url.
        :param url: URL to find data
        :returns: list of scanned urls, dict of title and description
        :rtype: list, dict
        """
        raw = await self._get(url)
        souped = self._souper(raw)
        scanned_urls = set()
        if souped:
            for url in self._scan_links(souped, url):
                scanned_urls.add(url)
            if souped.title:
                title = souped.title.string if souped.title.string else "None given"  # <title>
            else:
                title = "None given"
            desc = souped.find("meta", property="description") if souped.find("meta", property="description") else "None given"  # <meta description="">
            data = dict(title=title, desc=desc)
        else:
            data = None
        return scanned_urls, data

    def _export_csv(self, data):
        """Writes data to a .csv file in the working directory.
        :param data: URL, depth, title, description to write
        :type data: dict
        """
        with open("crawler.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["url", "depth", "title", "desc"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)

    async def crawl(self):
        """Main crawling function. Crawls within self.depth, starts from self.seed.
        """
        to_scan = [self.seed]
        data = []
        for depth in range(self.depth + 1):  # crawling depth
            for url in to_scan:
                robotlink = urljoin(url, "/robots.txt")
                robot = await self._get(robotlink)
                if self._robot_parser(robot, url):  # robot.txt politeness
                    got_data = await self._find_data(url)
                    new_links, page_data = got_data[0], got_data[1]
                    print(url)
                    if url not in self.found_urls:  # exclude scanned URLs
                        to_scan.extend(new_links)

                    if new_links and page_data:  # both must not be None to write
                        datadict = dict(url=url, depth=depth)
                        datadict.update(page_data)
                        data.append(datadict)

                    for x in new_links:
                        self.found_urls.add(x)
                else:
                    print(f"Caught in {robotlink}")
        self._export_csv(data)
        await self.session.close()
    
loop = asyncio.get_event_loop()
c = Crawler("https://docs.python.org", 5, 100)
loop.run_until_complete(c.crawl())
loop.close
