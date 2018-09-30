import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
import csv

class Crawler:

    def __init__(self, seed, depth, maxi:100):
        self.seed = seed
        self.depth = depth
        self.semaphore = asyncio.BoundedSemaphore(maxi)
        self.found_urls = set()
        self.session = aiohttp.ClientSession()

    async def _get(self, url):
        async with self.semaphore:
            try:
                async with self.session.get(url, timeout = 10) as r:
                    raw_html = await r.read()
                    return raw_html
            except Exception as e:
                print(f"Could not HTTP GET {url}: {e}")

    def _robot_parser(self, txt, url):
        parser = RobotFileParser()
        if txt:
            parser.parse(txt.decode("ascii", "replace").splitlines())
            return parser.can_fetch("*", url)
        else:
            return True

    def _souper(self, raw_html):
        if raw_html:
            souped_html = BeautifulSoup(raw_html, "html.parser")
            return souped_html

    def link_normalize(self, raw_link, url):
        if raw_link:
            abs_url = urljoin(url, raw_link)
            return abs_url
    
    def scan_links(self, souped_html, url):
        scanned_urls = []
        for rawlink in souped_html.find_all("a"):
            link = rawlink.get("href")
            abs_url = self.link_normalize(link, url)
            if abs_url:
                scanned_urls.append(abs_url)
        return scanned_urls
    
    async def links(self, url):
        raw = await self._get(url)
        souped = self._souper(raw)
        scanned_urls = set()
        if souped:
            for url in self.scan_links(souped, url):
                scanned_urls.add(url)
            if souped.title:
                title = souped.title.string if souped.title.string else "None given"
            else: 
                title = "None given"
            desc = souped.find("meta", property="description") if souped.find("meta", property="description") else "None given"
            data = dict(title=title, desc=desc)
        else:
            data = None
        return scanned_urls, data

    def export_csv(self, data):
        with open("crawler.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["url", "depth", "title", "desc"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)

    async def crawl(self):
        to_scan = [self.seed]
        data = []
        for depth in range(self.depth + 1):
            for url in to_scan:
                robotlink = urljoin(url, "/robots.txt")
                robot = await self._get(robotlink)
                if self._robot_parser(robot, url):
                    got_links = await self.links(url)
                    new_links, page_data = got_links[0], got_links[1]
                    print(url)
                    if url not in self.found_urls:
                        to_scan.extend(new_links)

                    if new_links and page_data:
                        datadict = dict(url=url, depth=depth)
                        datadict.update(page_data)
                        data.append(datadict)

                    for x in new_links:
                        self.found_urls.add(x)
                else:
                    print(f"Caught in {robotlink}")
        self.export_csv(data)
        await self.session.close()
    
loop = asyncio.get_event_loop()
c = Crawler("https://python.org", 5, 100)
loop.run_until_complete(c.crawl())
loop.close

