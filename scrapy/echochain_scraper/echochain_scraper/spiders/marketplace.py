import scrapy
from pathlib import Path
from scrapy.http import HtmlResponse


class MarketplaceSpider(scrapy.Spider):
    name = "marketplace"
    allowed_domains = []

    async def start(self):
        project_root = Path(__file__).resolve().parents[4]

        html_file = (
            project_root
            / "data"
            / "mock"
            / "marketplace"
            / "marketplace.html"
        )

        html_content = html_file.read_bytes()

        response = HtmlResponse(
            url=html_file.resolve().as_uri(),
            body=html_content,
            encoding="utf-8",
        )

        for item in self.parse(response):
            yield item

    def parse(self, response):
        for listing in response.css("div.listing"):
            yield {
                "title": listing.css("h2.title::text").get(),
                "price": listing.css("span.price::text").get(),
                "condition": listing.css("span.condition::text").get(),
                "seller": listing.css("span.seller::text").get(),
                "marketplace": listing.css("span.marketplace::text").get(),
                "listing_url": listing.css("a.listing-link::attr(href)").get(),
                "listing_date": listing.css("span.listing-date::text").get(),
            }