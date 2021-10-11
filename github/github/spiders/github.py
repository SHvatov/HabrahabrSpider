import csv
import dataclasses
import re
import time
from datetime import datetime
from typing import Optional, Iterator, Dict, Any, List
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from bs4 import BeautifulSoup
from scrapy import Spider, signals
from scrapy.http import Response, Request


@dataclasses.dataclass
class GitHubTechnologyData:
    CSV_COLUMNS = ["Link", "Global topic", "Technology name",
                   "Created by", "Released", "Total repositories"]

    link: str
    topic: str
    technology: str

    created_by: Optional[str]
    release_date: Optional[str]
    repositories: int

    def __iter__(self):
        return [self.link, self.topic, self.technology,
                self.created_by, str(self.release_date), self.repositories]


# noinspection DuplicatedCode
# todo: extract duplicate code into abstract common class
class GitHubTopicsSpider(Spider):
    __BASE_URL = "https://github.com"
    __SEARCH_QUERY_PATTERN: str = "https://github.com/search?p={page}&q={query}&type=Topics"
    __MAX_PAGES_TO_CRAWL = 10

    __QUERY_ARG = "query"
    __DIR_ARG: str = "dir"

    name: str = "github-topics"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # acquire config data from arguments
        if GitHubTopicsSpider.__QUERY_ARG not in kwargs.keys():
            raise ValueError("Mandatory \"query\" parameter is absent")

        self.__query = str(kwargs[GitHubTopicsSpider.__QUERY_ARG])
        self.__path_to_dir = str(kwargs[GitHubTopicsSpider.__DIR_ARG]) \
            if GitHubTopicsSpider.__DIR_ARG in kwargs.keys() else None

        if self.__path_to_dir is not None:
            self.__path_to_csv_dir = f"{self.__path_to_dir}/csv"
            self.__path_to_failed_dir = f"{self.__path_to_dir}/failed"
        else:
            self.__path_to_csv_dir = None
            self.__path_to_failed_dir = None

        self.__topic_data = list()
        self.__total_pages_to_parse = self.__parse_total_pages_num(self.__get_url())
        if self.__total_pages_to_parse > GitHubTopicsSpider.__MAX_PAGES_TO_CRAWL:
            self.__total_pages_to_parse = GitHubTopicsSpider.__MAX_PAGES_TO_CRAWL
        print(f"Total pages to parse: {self.__total_pages_to_parse}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(GitHubTopicsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.on_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self) -> Iterator[Request]:
        for page in range(1, self.__total_pages_to_parse + 1):
            # for page in range(1, 2):
            yield Request(self.__get_url(page))

    def parse(self, response: Response, **kwargs: Dict[Any, Any]) -> None:
        page = self.__retrieve_page_number_from_url(response.url)
        technologies = self.__parse_technologies(response.body)
        self.__topic_data.extend(technologies)
        print(f"Processed page #{page}, added {len(technologies)} technologies, related to this topic. "
              f"Total is {len(self.__topic_data)}")

    # noinspection PyUnusedLocal
    def on_closed(self, spider: Spider):
        if self.__path_to_csv_dir is not None:
            filename = f"{self.__path_to_csv_dir}/" \
                       f"{GitHubTopicsSpider.name}-results-{datetime.today().strftime('%Y-%m-%d')}.csv"
            print(f"Writing {len(self.__topic_data)} records to csv file with name {filename}")
            with open(filename, 'w', encoding="UTF-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(GitHubTechnologyData.CSV_COLUMNS)
                writer.writerows([list(el.__iter__()) for el in self.__topic_data])
        pass

    def __get_url(self, page: int = 1) -> str:
        return GitHubTopicsSpider.__SEARCH_QUERY_PATTERN.format(page=page, query=self.__query)

    def __parse_technologies(self, body: str) -> List[GitHubTechnologyData]:
        page = BeautifulSoup(body, "html.parser")
        topic_divs = page.find_all("div", class_="topic-list-item")

        parsed_technologies = list()
        for parent_div in topic_divs:
            sub_divs = parent_div.find_all('div')
            if len(sub_divs) != 0:
                links = sub_divs[0].find_all('a')
                if len(links) != 0:
                    href = links[0]['href']
                    parsed_technologies.append(self.__parse_technology(href))

        return [el for el in parsed_technologies if el is not None]

    def __parse_technology(self, link: str) -> Optional[GitHubTechnologyData]:
        print(f"Started processing technology with url: {link}")
        actual_url = GitHubTopicsSpider.__BASE_URL + link
        page = GitHubTopicsSpider.__open_page(actual_url)

        try:
            technology = str(page.find("h1", class_="h1").contents[0]).strip()

            side_bar = page.find("div", class_="col-md-4 col-lg-3")
            additional_data = side_bar.find_all("p", class_="mb-1")
            additional_data = [
                str(_.contents[2]).strip().replace('\"', '') if len(_.contents) >= 3 else None
                for _ in additional_data
            ]
            created_by = additional_data[0] if len(additional_data) >= 1 else None
            release_date = additional_data[1] if len(additional_data) >= 2 else None

            created_by = None if release_date is None else created_by
            release_date = None if created_by is None else release_date

            repositories = self.__retrieve_numbers_from_str(
                str(page.find("h2", class_="h3 color-text-secondary").contents[0])
            )
            if len(repositories) == 2:
                repositories = repositories[0] * 1000 + repositories[1]
            else:
                repositories = repositories[0]

            data = GitHubTechnologyData(link, self.__query, technology,
                                        created_by, release_date, repositories)
            print(f"Processed technology with url: {actual_url}. Parsed data: {data}")
            return data
        except Exception as ex:
            print(f"Encountered following exception ({ex.__class__}) when attempting to parse data: {ex}")
            if self.__path_to_failed_dir is not None:
                filename = f"{self.__path_to_failed_dir}/" \
                           f"{GitHubTopicsSpider.name}-failed-{link.replace('/', '-')}.html"
                print(f"Saving failed to parse html to {filename}")
                with open(filename, 'w', encoding="UTF-8") as f:
                    f.write(page.prettify())
            return None

    @staticmethod
    def __retrieve_numbers_from_str(string_with_numbers: str) -> List[int]:
        s = re.sub(r"\D", " ", string_with_numbers)
        return [int(d) for d in s.split() if d.isdigit()]

    @staticmethod
    def __retrieve_page_number_from_url(url: str) -> int:
        return int(url[url.find('p=') + 2:url.find('&')])

    @staticmethod
    def __open_page(url: str, previously_failed: bool = False) -> BeautifulSoup:
        try:
            page = urlopen(url)
        except HTTPError as e:
            print(f"Server returned the following HTTP error code while "
                  f"performing a request to {e.url}: {e.code}")
            if e.code == 429 and not previously_failed:
                time.sleep(30)
                return GitHubTopicsSpider.__open_page(url, True)
            raise e
        except URLError as e:
            print(f"Could no find a server, which is associated "
                  f"with the following url: {url}")
            raise e
        return BeautifulSoup(page.read(), "html.parser")

    @staticmethod
    def __parse_total_pages_num(url: str) -> int:
        page = GitHubTopicsSpider.__open_page(url)
        navigation_div = page.find("div", class_="pagination")
        pages = navigation_div.find_all("a", recursive=False)[:-1]

        max_page = -1
        for page_link in pages:
            actual_page = int(page_link.contents[0])
            if actual_page > max_page:
                max_page = actual_page

        if max_page != 1:
            return max_page
        raise ValueError(f"Could not parse a total number of the pages to process from {url}")
