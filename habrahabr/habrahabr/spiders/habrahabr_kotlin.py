import csv
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Any, Optional, Set, Dict, List
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from bs4 import BeautifulSoup
from scrapy import Spider, Request, signals
from scrapy.http import Response


@dataclass
class HabrahabrArticleData:
    CSV_COLUMNS = ["Link", "Tags", "Hubs", "Unique user", "Company name", "Username",
                   "Number of comments", "Number of positive votes", "Number of negative votes",
                   "Number of views", "Number of bookmarks"]

    link: str  # unique identifier of the article
    tags: Set[str]  # list of associated tags
    hubs: Set[str]  # list of associated hubs

    is_unique_user: bool  # whether this article is provided by a unique user or a company
    company: Optional[str]  # company which created this post
    user: str  # specific user, whi created the post

    comments: int  # number of the comments under the article
    positive_votes: int  # number of positive votes
    negative_votes: int  # number of negative votes
    views: int  # number of view
    bookmarks: int  # number of the people, who has bookmarked the article

    def __iter__(self):
        return [self.link, ",".join(self.tags), ",".join(self.hubs),
                self.is_unique_user, self.company, self.user,
                self.comments, self.positive_votes, self.negative_votes,
                self.views, self.bookmarks]


class HabrahabrKotlinSpider(Spider):
    __HABR_BASE_URL: str = "https://habr.com"
    __KOTLIN_HABR_BASE_URL: str = f"{__HABR_BASE_URL}/ru/hub/kotlin/"
    name: str = "habrahabr-kotlin"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__articles_data = list()
        self.__total_pages_to_parse = self.__parse_total_pages_num()
        print(f"Total pages to pare: {self.__total_pages_to_parse}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(HabrahabrKotlinSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.on_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self) -> Iterator[str]:
        for page in range(1, self.__total_pages_to_parse):
            yield Request(HabrahabrKotlinSpider.__KOTLIN_HABR_BASE_URL + f"page{page}/")

    def parse(self, response: Response, **kwargs: Dict[Any, Any]) -> None:
        page = self.__retrieve_page_number_from_url(response.url)
        articles = self.__parse_articles(response.body)
        self.__articles_data.extend(articles)
        print(f"Processed page #{page}, added {len(articles)} articles. Total is {len(self.__articles_data)}")

    # noinspection PyUnusedLocal
    def on_closed(self, spider: Spider):
        filename = f"{HabrahabrKotlinSpider.name}-results-{datetime.today().strftime('%Y-%m-%d')}.csv"
        print(f"Writing {len(self.__articles_data)} records to csv file with name {filename}")
        with open(filename, 'w', encoding="UTF-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(HabrahabrArticleData.CSV_COLUMNS)
            writer.writerows([list(el.__iter__()) for el in self.__articles_data])

    @staticmethod
    def __parse_articles(body: str) -> List[HabrahabrArticleData]:
        page = BeautifulSoup(body, "html.parser")
        links = page.find_all("a", class_="tm-article-snippet__readmore")
        parsed_articles = [HabrahabrKotlinSpider.__parse_article(link["href"]) for link in links]
        return [el for el in parsed_articles if el is not None]

    @staticmethod
    def __parse_article(link: str) -> Optional[HabrahabrArticleData]:
        print(f"Started processing article with url: {link}")
        actual_url = HabrahabrKotlinSpider.__HABR_BASE_URL + link
        page = HabrahabrKotlinSpider.__open_page(actual_url)

        try:
            # parsing links from the page
            tags, hubs = list(), list()
            for link_item in page.find_all("a", class_="tm-article-body__tags-item-link"):
                link_item_name = str(link_item.string).strip()
                if str(link_item["href"]).count("ru/hub") != 0:
                    hubs.append(link_item_name)
                else:
                    tags.append(link_item_name)
            tags, hubs = set(tags), set(hubs)

            # parsing user data
            is_unique_user = link.count("ru/company") != 0
            company = link[link.find("company") + len("company/"):link.find("/blog")] if is_unique_user else None
            user = str(page.find("a", class_="tm-user-info__username").string).strip()

            # parsing different stats
            comments = page.find("span", class_="tm-article-comments-counter-link__value").string
            comments = HabrahabrKotlinSpider.__retrieve_numbers_from_str(comments)[0]

            # parsing number of votes
            total_votes = page.find("span", class_="tm-votes-meter__value_medium")
            if "title" in total_votes.contents:
                total_votes_title = total_votes["title"]
                _, positive_votes, negative_votes = HabrahabrKotlinSpider.__retrieve_numbers_from_str(total_votes_title)
            else:
                positive_votes = negative_votes = 0

            # parsing number of views
            views_str = str(page.find("span", class_="tm-icon-counter__value").string)
            if views_str.count("K") != 0:
                views_str = views_str.replace("K", "")
                views = int(float(views_str) * 1000)
            else:
                views = int(views_str)

            bookmarks = int(page.find("span", class_="bookmarks-button__counter").string)

            data = HabrahabrArticleData(
                link, tags, hubs,
                is_unique_user, company, user,
                comments, positive_votes, negative_votes,
                views, bookmarks
            )
            print(f"Processed article with url: {actual_url}. Parsed data: {data}")
            return data
        except Exception as ex:
            print(f"Encountered following exception ({ex.__class__}) when attempting to parse data: {ex}")
            filename = f"{HabrahabrKotlinSpider.name}-failed-{link.replace('/', '-')}.html"
            print(f"Saving failed to parse html to {filename}")
            with open(filename, 'w', encoding="UTF-8") as f:
                f.write(page.prettify())

    @staticmethod
    def __retrieve_numbers_from_str(string_with_numbers: str) -> List[int]:
        s = re.sub(r"\D", " ", string_with_numbers)
        return [int(d) for d in s.split() if d.isdigit()]

    @staticmethod
    def __retrieve_page_number_from_url(url: str) -> int:
        return int(url[url.find("page") + len("page"):-1])

    @staticmethod
    def __open_page(url) -> BeautifulSoup:
        try:
            page = urlopen(url)
        except HTTPError as e:
            print(f"Server returned the following HTTP error code while "
                  f"performing a request to {e.url}: {e.code}")
            raise e
        except URLError as e:
            print(f"Could no find a server, which is associated "
                  f"with the following url: {url}")
            raise e
        return BeautifulSoup(page.read(), "html.parser")

    @staticmethod
    def __parse_total_pages_num() -> int:
        page = HabrahabrKotlinSpider.__open_page(HabrahabrKotlinSpider.__KOTLIN_HABR_BASE_URL)
        divs = page.find_all("a", class_="tm-pagination__page")

        max_page = -1
        for div in divs:
            page = int(div.contents[0])
            if page > max_page:
                max_page = page

        if max_page != 1:
            return max_page
        raise ValueError(f"Could not parse a total number of the pages "
                         f"to process from {HabrahabrKotlinSpider.__KOTLIN_HABR_BASE_URL}")
