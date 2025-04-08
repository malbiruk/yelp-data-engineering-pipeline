import asyncio
import html
import json
import logging
import random
import re
import urllib.parse
from contextlib import asynccontextmanager
from io import StringIO
from pathlib import Path

import aiohttp
import pandas as pd
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from jsonpath_ng import parse
from pydantic import BaseModel, Field
from rich.logging import RichHandler
from tenacity import retry, stop_after_attempt, wait_random

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()],
)
logger = logging.getLogger(__name__)


def retry_with_logging(
        n_retries: int = 5,
        min_delay: int = 2,
        max_delay: int = 8):
    """
    Decorator for retrying a function call with logging.

    Retries a function up to `n_retries` times, with a random delay
    between `min_delay` and `max_delay` seconds before each retry.
    """
    return retry(
        stop=stop_after_attempt(n_retries),
        wait=wait_random(min=min_delay, max=max_delay),
        before_sleep=lambda retry_state: logger.warning(
            "Retry %s/%s in %s seconds due to %s",
            retry_state.attempt_number,
            n_retries,
            retry_state.next_action.sleep,
            retry_state.outcome.exception(),
        ),
    )


async def create_session(headers: dict | None = None,
                         user_agents_list: list[str] | None = None,
                         proxies_list: list[str] | None = None) -> aiohttp.ClientSession:
    if user_agents_list:
        if headers:
            headers.update({"User-Agent": random.choice(user_agents_list)})
        else:
            headers = {"User-Agent": random.choice(user_agents_list)}

    proxy = None
    if proxies_list:
        proxy = random.choice(proxies_list)
        if not proxy.startswith("http://") and not proxy.startswith("https://"):
            proxy = "http://" + proxy

    timeout = ClientTimeout(total=30)

    return aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
        proxy=proxy if proxies_list else None,
    )


@asynccontextmanager
async def get_session(headers, user_agents, proxies_list):
    session = await create_session(headers, user_agents, proxies_list)
    try:
        yield session
    finally:
        await session.close()


def extract_urls_from_search_page(json_text: str) -> list[dict[str, str]]:
    """
    Extracts business URLs and metadata from a Yelp search result JSON.

    Parses the JSON response to retrieve business IDs, rankings, names, and
    URLs while filtering out advertisements.

    Raises:
        ValueError: If a captcha is detected or the page fails to load.
    """
    business_pattern = re.findall(
        r'{\s*"bizId"\s*:\s*"([^"]+)"\s*,\s*"searchResultBusiness"\s*:\s*{.*?'
        r'"ranking"\s*:\s*(\d+).*?"isAd"\s*:\s*(true|false).*?"name"\s*:\s*"([^"]+).*?'
        r'"businessUrl"\s*:\s*"([^"]+)"',
        json_text, re.DOTALL,
    )

    businesses = []
    for match in business_pattern:
        biz_id, ranking, is_ad, name, business_url = match
        businesses.append({
            "bizId": biz_id,
            "ranking": int(ranking),
            "isAd": is_ad == "true",
            "name": name,
            "businessUrl": business_url,
        })

    businesses = [b for b in businesses if not b["isAd"]]
    for b in businesses:
        b["businessUrl"] = "https://www.yelp.com" + b["businessUrl"].rsplit("?", 1)[0]
        b.pop("isAd")
    if not businesses:
        if 'src="https://ct.captcha-delivery.com/i.js"' in json_text:
            raise ValueError("Encountered captcha")
        raise ValueError("Page didn't load successfully")
    return businesses


@retry_with_logging()
async def scrape_single_search_page(
    find_desc: str,
    find_loc: str,
    start: int,
    headers: dict,
    user_agents: list[str],
    proxies_list: list[str],
    timeout: int,
) -> list[dict] | None:
    await asyncio.sleep(random.uniform(0, 2))
    async with get_session(headers, user_agents, proxies_list) as session:
        # First request to get cookies/session data
        async with session.get(
            "https://www.yelp.com/search",
            params={"find_desc": find_desc, "find_loc": find_loc},
            timeout=timeout,
        ) as response:
            url = response.url
            await response.text()

        headers = {
            "accept": "application/json",
            "accept-language": "en;q=0.6",
            "content-type": "application/json",
            "referer": url,
            "x-requested-with": "XMLHttpRequest",
            "Accept-Encoding": "gzip, deflate",
        }

        # Actual search request
        await asyncio.sleep(random.uniform(2, 5))
        async with session.get(
            "https://www.yelp.com/search/snippet",
            params={"find_desc": find_desc, "find_loc": find_loc, "start": start},
            headers={"Referer": "https://www.yelp.com/"},
            timeout=timeout,
        ) as response:
            text = await response.text()
            if "excessivePaging" in text:  # meaning that the relevant search pages ended
                return None
            return extract_urls_from_search_page(text)


async def scrape_search_pages(
    find_desc: str,
    find_loc: str,
    headers: dict,
    user_agents: list[str],
    proxies_list: list[str],
    timeout: int,
    outfile: Path,
    batch_size: int = 10,
) -> None:
    """
    Scrapes multiple Yelp search result pages to gather business listings.

    This function paginates through Yelp's search results, extracting
    business URLs and storing them in a file.

    Args:
        find_desc: The type of business to search for (e.g., "Restaurants").
        find_loc: The location for the search (e.g., "Las Vegas").
        outfile: Path to the file where business listings will be saved.
        batch_size: Number of pages to scrape concurrently
                    (each page includes 10 entries).
    """
    start = 0
    while True:
        logger.info("Processing batch starting from %s", start)

        tasks = [
            asyncio.create_task(
                scrape_single_search_page(
                    find_desc=find_desc,
                    find_loc=find_loc,
                    start=start + (i * 10),
                    headers=headers,
                    user_agents=user_agents,
                    proxies_list=proxies_list,
                    timeout=timeout,
                ),
            )
            for i in range(batch_size)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        empty_page_found = False
        for result in results:
            if isinstance(result, Exception):
                logger.error("A request failed: %s", result, exc_info=True)
                continue

            if result is None:  # None is returned only if excessivePaging is in response text
                empty_page_found = True
                break

            with outfile.open("a") as f:
                for business in result:
                    json.dump(business, f)
                    f.write("\n")

        if empty_page_found:
            break

        start += batch_size * 10


class BusinessInfo(BaseModel):
    """
    data model for a restaurant
    """
    name: str
    website: str | None
    phone_number: str | None
    open_hours: list[dict[str, str]]
    address: str | None
    food_category: list[str]
    price: str | None = Field(pattern=r"^\${1,4}$", description="Should be $, $$, $$$, or $$$$")
    health_score: str | None = Field(
        pattern=r"^[A-Z]$", description="Exactly one uppercase letter")
    amenities: list[dict[str, str | bool]]
    highlights: list[str]
    related_search_terms: list[str]


def _get_website(soup: BeautifulSoup) -> str:
    """
    Extracts the business website URL from the Yelp business page.

    Returns:
        The website URL as a string if available, otherwise None.
    """
    if not soup.find("p", string="Business website"):
        return None
    link_tag = soup.find("p", string="Business website").find_next_sibling().find("a")
    parsed_url = urllib.parse.parse_qs(urllib.parse.urlparse(link_tag["href"]).query)
    return parsed_url.get("url", [None])[0]


def _process_hours_table(hours_table: pd.DataFrame) -> list[dict[str, str]]:
    hours_table = hours_table.dropna(how="all").iloc[:, :2]
    if hours_table.columns[0].startswith("Unnamed") and hours_table.columns[1].startswith("Unnamed"):
        hours_table.columns = ["weekday", "open_hours"]
    else:
        raise ValueError("First two columns already have names!")
    return hours_table.to_dict(orient="records")


def _get_open_hours(soup: BeautifulSoup) -> list[dict[str, str]]:
    hours_table = soup.find(class_=re.compile("^hours-table_"))
    try:
        hours_table = pd.read_html(StringIO(str(hours_table)))[0]
        return _process_hours_table(hours_table)

    except ValueError:
        return []


def _extract_script_json(soup: BeautifulSoup) -> dict | None:
    """
    Extracts and parses JSON data from the embedded `<script>` tag on the page.

    Returns:
        A dictionary containing Yelp's Apollo state JSON data, if available.
    """
    script_tag = soup.find("script", {"type": "application/json", "data-apollo-state": True})
    if script_tag:
        return json.loads(html.unescape(script_tag.string)[4:-3])
    return None


def _get_amenities(soup: BeautifulSoup, json_data: dict | None) -> list[dict[str, bool]]:
    """
    Extracts business amenities from Yelp's JSON data.

    Parses structured JSON from the business page to retrieve available
    amenities and their statuses.

    Returns:
        A list of dictionaries with `amenity` name and `is_available` boolean.
    """
    if not soup.select_one('section[aria-label="Amenities and More"]') or not json_data:
        return []

    jsonpath_expr = parse(r'$..["organizedProperties({\"clientPlatform\":\"WWW\"})"]')
    matches = [match.value for match in jsonpath_expr.find(json_data)]
    if matches[0]:
        return [{"amenity": amenity["displayText"], "is_available": amenity["isActive"]}
                for amenity in matches[0][0]["properties"]]
    return []


def _get_highlights(json_data: dict) -> list[str]:
    jsonpath_expr = parse(r'$..["businessHighlights"]')
    matches = [match.value for match in jsonpath_expr.find(json_data)]
    if matches:
        return [highlight["title"] for highlight in matches[0]]
    return []


def _get_related_search_terms(json_data: dict | None) -> list[str]:
    if not json_data:
        return []
    jsonpath_expr = parse(
        r'$..["associatedSearchesV2({\"type\":\"people_found_biz_search_type_v1\"})"]')
    matches = [match.value for match in jsonpath_expr.find(json_data)]
    if matches[0]:
        return [item["searchPhrase"] for item in matches[0]]
    return []


def extract_data_from_business_page(
    webpage_html: str,
    yelp_biz_data: dict,
) -> BusinessInfo:
    """
    Process pre-fetched webpage HTML and business data to extract business information.
    """
    soup = BeautifulSoup(webpage_html, "lxml")
    if not soup.select_one("h1"):
        if 'src="https://ct.captcha-delivery.com/i.js"' in webpage_html:
            raise ValueError("Encountered captcha")
        raise ValueError("The page contents didn't load successfully")

    script_json = _extract_script_json(soup)

    return BusinessInfo(
        name=soup.select_one("h1").text,
        website=_get_website(soup),
        phone_number=(None if not (pn_element := soup.find("p", string="Phone number"))
                      else (pn_element.find_next_sibling().text)),
        open_hours=_get_open_hours(soup),
        address=(None if not (gd_element := soup.find("a", string="Get Directions"))
                 else gd_element.parent.find_next_sibling().text),
        food_category=[el.text.strip().replace(",", "")
                       for el in soup.select("[data-testid='BizHeaderCategory']")],
        price=(soup.select_one('[data-testid="photoHeader"]')
               .find(string=re.compile(r"^\${1,4}$"))),
        health_score=(None if not (hs := soup.find("a", string="Health Score"))
                      else hs.parent.find_next_sibling().text),
        amenities=_get_amenities(soup, script_json),
        highlights=_get_highlights(yelp_biz_data),
        related_search_terms=_get_related_search_terms(script_json),
    )


@retry_with_logging()
async def scrape_single_business(
    business: dict,
    headers: dict,
    user_agents: list[str],
    proxies_list: list[str],
    timeout: int,
    outfile: Path,
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore, get_session(headers, user_agents, proxies_list) as session:
        url = business["businessUrl"]

        await asyncio.sleep(random.uniform(0, 5))

        async with session.get(url, timeout=timeout) as response:
            webpage_html = await response.text()

        headers = {
            "accept": "application/json",
            "accept-language": "en;q=0.6",
            "content-type": "application/json",
            "referer": url,
            "x-requested-with": "XMLHttpRequest",
            "Accept-Encoding": "gzip, deflate",
        }

        # Get business data
        async with session.get(
            f"https://www.yelp.com/biz/{business['bizId']}/props",
            headers=headers,
            timeout=timeout,
        ) as response:
            yelp_biz_data = await response.json()

        business_info = extract_data_from_business_page(webpage_html, yelp_biz_data)
        business_data = {k: v for k, v in business.items() if k in ("bizId", "ranking")}
        result = business_data | business_info.model_dump()

        async with asyncio.Lock():
            with outfile.open("a") as f:
                json.dump(result, f)
                f.write("\n")


async def scrape_businesses(
    businesses_to_scrape: list[dict],
    headers: dict,
    user_agents: list[str],
    proxies_list: list[str],
    timeout: int,
    outfile: Path,
    concurrency: int = 10,
) -> None:
    """
    Scrapes detailed information from individual Yelp business pages.

    This function fetches business details (e.g., hours, amenities, website)
    and saves them in a structured format.

    Args:
        businesses_to_scrape: List of businesses obtained from search results.
        concurrency: Number of business pages to scrape in parallel.
    """
    semaphore = asyncio.Semaphore(concurrency)

    tasks = [
        scrape_single_business(
            business=business,
            headers=headers,
            user_agents=user_agents,
            proxies_list=proxies_list,
            timeout=timeout,
            outfile=outfile,
            semaphore=semaphore,
        )
        for business in businesses_to_scrape
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error("Scraping failed: %s", result, exc_info=True)
            continue


USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
]

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en;q=0.6",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}


async def main(
    find_desc: str = "Restaurants",
    find_loc: str = "Las Vegas",
    timeout: int = 10,
    outfile: Path | str = "results.ndjson",
    headers: dict[str, str] | None = None,
    user_agents: list[str] | None = None,
    proxies_list: list[str] | None = None,
) -> None:
    """
    Scrapes Yelp business data for given search criteria and location.

    Uses a two-phase approach:
    1. Scrapes search results to get business URLs
    2. Scrapes individual business pages for detailed information

    Results are saved in NDJSON format with partial progress preserved.
    """
    if headers is None:
        headers = HEADERS
    if user_agents is None:
        user_agents = USER_AGENTS

    outfile = Path(outfile)
    tmpfile = Path("businesses.ndjson")

    # Obtain all business links from search
    if not tmpfile.exists():
        logger.info("Scraping search results")
        await scrape_search_pages(
            find_desc=find_desc,
            find_loc=find_loc,
            headers=headers,
            user_agents=user_agents,
            proxies_list=proxies_list,
            timeout=timeout,
            outfile=tmpfile,
            batch_size=10,
        )

    # Scrape only businesses not yet scraped
    with tmpfile.open() as f:
        businesses_to_scrape = [json.loads(line) for line in f]

    if outfile.exists():
        with outfile.open() as f:
            businesses_scraped = {json.loads(line)["bizId"] for line in f}
        businesses_to_scrape = [b for b in businesses_to_scrape
                                if b["bizId"] not in businesses_scraped]

    if businesses_to_scrape:
        logger.info("Scraping individual business pages")

    await scrape_businesses(
        businesses_to_scrape=businesses_to_scrape,
        headers=headers,
        user_agents=user_agents,
        proxies_list=proxies_list,
        timeout=timeout,
        outfile=outfile,
        concurrency=20,
    )

if __name__ == "__main__":
    with Path("proxies_list.txt").open() as f:
        proxies_list = f.readlines()

    asyncio.run(main(proxies_list=proxies_list))
