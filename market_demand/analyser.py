import csv
import dataclasses
import itertools
import os
from datetime import datetime
from typing import Set, List
from urllib.parse import quote

from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor

from market_demand.spiders.github import GitHubTechnologyData, GitHubTopicsSpider
from market_demand.spiders.habrahabr import HabrahabrArticleData, HabrahabrArticlesSpider

dirname = os.path.dirname(__file__)
PATH_TO_GITHUB_DATA_DIR = os.path.join(dirname, '../data/github')
PATH_TO_HABR_DATA_DIR = os.path.join(dirname, '../data/habr')
PATH_TO_RESULTS_DIR = os.path.join(dirname, '../data/results')

PATH_TO_GITHUB_CSV_DIR = os.path.join(PATH_TO_GITHUB_DATA_DIR, './csv')

TECHNOLOGY = "JavaScript"


@dataclasses.dataclass
class CompanyStats:
    company: str
    articles: int


@dataclasses.dataclass
class TechnologyStats:
    CSV_COLUMNS = ["Link", "Topic", "Technology",
                   "Repositories", "Articles", "By Users", "By Companies",
                   "Total views", "Views per article",
                   "Total comments", "Comments per article",
                   "Avg. positive votes", "Avg. negative votes",
                   "Bookmarks per article", "Companies",
                   "Tags", "Hubs"]

    link: str
    topic: str
    technology: str

    repositories: int
    total_articles: int
    articles_create_by_users: int
    articles_create_by_companies: int

    total_views: int
    average_views: float

    total_comments: int
    average_comments: int

    average_positive_votes: float
    average_negative_votes: float
    average_bookmarks: int

    common_tags: Set[str]
    common_hubs: Set[str]

    companies_stats: List[CompanyStats]

    def __iter__(self):
        return [self.link, self.topic, self.technology,
                self.repositories, self.total_articles,
                self.articles_create_by_users, self.articles_create_by_companies,
                self.total_views, self.average_views, self.total_comments, self.average_comments,
                self.average_positive_votes, self.average_negative_votes,
                self.average_bookmarks,
                str(sorted(self.companies_stats, key=lambda _: _.articles, reverse=True)),
                str(self.common_tags), str(self.common_hubs)]


def main():
    configure_logging()
    settings = get_project_settings()
    runner = CrawlerRunner(settings)

    @defer.inlineCallbacks
    def crawl_and_analyse():
        yield runner.crawl(GitHubTopicsSpider.name,
                           query=quote(TECHNOLOGY),
                           dir=PATH_TO_GITHUB_DATA_DIR,
                           habr_dir=PATH_TO_HABR_DATA_DIR)

        latest_file = None
        for file in os.scandir(PATH_TO_GITHUB_CSV_DIR):
            # noinspection PyUnresolvedReferences
            if file.is_file() and file.path.endswith(".csv"):
                # noinspection PyUnresolvedReferences
                if latest_file is None or file.path > latest_file:
                    # noinspection PyUnresolvedReferences
                    latest_file = file.path

        technologies = list()
        with open(latest_file, 'r', newline='', encoding="UTF-8") as csv_file:
            reader = csv.reader(csv_file)
            next(reader, None)
            for link, topic, technology, _, _, repos, path_to_data in reader:
                technologies.append(
                    GitHubTechnologyData(
                        link, topic, technology,
                        None, None,
                        int(repos), path_to_data
                    )
                )

        for technology in technologies:
            yield runner.crawl(HabrahabrArticlesSpider.name,
                               query=quote(technology.technology),
                               dir=PATH_TO_HABR_DATA_DIR)

        technology_to_id = {index: technology for index, technology in enumerate(technologies)}
        articles_data_to_tech_id = dict()
        for index, technology in technology_to_id.items():
            articles = list()
            with open(technology.path_to_data, 'r', newline='', encoding="UTF-8") as csv_file:
                reader = csv.reader(csv_file)
                next(reader, None)
                for link, tags, hubs, unique, company, user, comments, pos_votes, neg_votes, views, bookmarks in reader:
                    articles.append(
                        HabrahabrArticleData(
                            link, set(str(tags).replace('\"', '').lower().split(',')),
                            set(str(hubs).replace('\"', '').split(',')),
                            unique == 'True', company, user,
                            int(comments), int(pos_votes), int(neg_votes),
                            int(views), int(bookmarks)
                        )
                    )
            articles_data_to_tech_id[index] = articles

        technology_stats = list()
        for index, technology in technology_to_id.items():
            articles = articles_data_to_tech_id[index]
            if len(articles) == 0:
                continue

            total_articles = len(articles)
            articles_create_by_users = sum([1 if article.is_unique_user else 0 for article in articles])
            articles_create_by_companies = total_articles - articles_create_by_users

            total_views = sum([article.views for article in articles])
            average_views = float(total_views) / total_articles

            total_comments = sum([article.comments for article in articles])
            average_comments = total_comments // total_articles

            average_positive_votes = sum([article.positive_votes for article in articles]) / float(total_articles)
            average_negative_votes = sum([article.negative_votes for article in articles]) / float(total_articles)
            average_bookmarks = sum([article.bookmarks for article in articles]) // total_articles

            common_tags = set(itertools.chain(*[article.tags for article in articles]))
            common_hubs = set(itertools.chain(*[article.hubs for article in articles]))

            companies = set([article.company for article in articles if not article.is_unique_user])
            companies_data = [CompanyStats(c, len([_ for _ in articles if _.company == c])) for c in companies]

            technology_stats.append(
                TechnologyStats(
                    technology.link, technology.topic, technology.technology, technology.repositories,
                    total_articles, articles_create_by_users, articles_create_by_companies,
                    total_views, average_views, total_comments, average_comments,
                    average_positive_votes, average_negative_votes, average_bookmarks,
                    common_tags, common_hubs, companies_data
                )
            )

        filename = f"{PATH_TO_RESULTS_DIR}/" \
                   f"results-{datetime.today().strftime('%Y-%m-%d')}.csv"
        with open(filename, 'w', encoding="UTF-8", newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(TechnologyStats.CSV_COLUMNS)
            writer.writerows([list(el.__iter__()) for el in technology_stats])

        # noinspection PyUnresolvedReferences
        reactor.stop()

    crawl_and_analyse()

    # noinspection PyUnresolvedReferences
    reactor.run()


if __name__ == '__main__':
    main()
