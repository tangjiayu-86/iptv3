import ast
import asyncio
import re
from functools import partial
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "WEBCAST"

CACHE_FILE = Cache(TAG, exp=12_600)

BASE_URLS = {
    "MLB": "https://mlbwebcast.com",
    # "NFL": "https://nflwebcast.com",
    # "NHL": "https://slapstreams.com",
}


def fix_event(s: str) -> str:
    return " vs ".join(s.split("@"))


async def process_event(
    url: str,
    url_num: int,
    sport: str,
) -> str | None:

    if not (event_data := await network.request(url, log=log)):
        log.warning(f"URL {url_num}) Failed to load url.")
        return

    soup = HTMLParser(event_data.content)

    if not (iframe := soup.css_first('iframe[name="srcFrame"]')):
        log.warning(f"URL {url_num}) No iframe element found.")
        return

    if not (iframe_src := iframe.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe source found.")
        return

    if not (
        iframe_src_data := await network.request(
            iframe_src,
            headers={"Referer": url},
            log=log,
        )
    ):
        log.warning(f"URL {url_num}) Failed to load iframe source.")
        return

    pattern = re.compile(r'var\s+\w*=\[([^"]*)\];', re.I)

    if not (match := pattern.search(iframe_src_data.text)):
        log.warning(f"URL {url_num}) No Clappr source found.")
        return

    try:
        ev_id, ev_ts, ev_pt = ast.literal_eval(match[1])
    except ValueError:
        log.warning(f"URL {url_num}) Failed to parse event info.")
        return

    params: dict[str, int | str] = dict(zip(["id", "ts", "pt"], [ev_id, ev_ts, ev_pt]))

    if not (
        api_data := await network.request(
            urljoin(BASE_URLS[sport], "stream/check_stream.php"),
            headers={"Referer": iframe_src},
            params=params,
            log=log,
        )
    ):
        log.warning(f"URL {url_num}) Failed to make php request.")
        return

    elif (data := api_data.json()).get("error"):
        log.warning(f"URL {url_num}) Failed to make php request.")
        return

    log.info(f"URL {url_num}) Captured M3U8")

    return data.get("url")


async def get_events() -> list[dict[str, str]]:
    tasks = [network.request(url, log=log) for url in BASE_URLS.values()]

    results = await asyncio.gather(*tasks)

    events = []

    if not (
        soups := [(HTMLParser(html.content), html.url) for html in results if html]
    ):
        return events

    for soup, url in soups:
        sport = next((k for k, v in BASE_URLS.items() if v == url), "Live Event")

        for row in soup.css("tr.singele_match_date"):
            if not (vs_node := row.css_first("td.teamvs a")):
                continue

            event_name = vs_node.text(strip=True)

            for span in vs_node.css("span.mtdate"):
                date = span.text(strip=True)

                event_name = event_name.replace(date, "").strip()

            if not (href := vs_node.attributes.get("href")):
                continue

            events.append(
                {
                    "sport": sport,
                    "event": fix_event(event_name),
                    "link": href,
                }
            )

    return events


async def scrape() -> None:
    if cached_urls := CACHE_FILE.load():
        urls.update({k: v for k, v in cached_urls.items() if v["url"]})

        log.info(f"Loaded {len(urls)} event(s) from cache")

        return

    log.info(f'Scraping from "{' & '.join(BASE_URLS.values())}"')

    if events := await get_events():
        log.info(f"Processing {len(events)} URL(s)")

        now = Time.clean(Time.now())

        for i, ev in enumerate(events, start=1):
            handler = partial(
                process_event,
                url=(link := ev["link"]),
                url_num=i,
                sport=(sport := ev["sport"]),
            )

            url = await network.safe_process(
                handler,
                url_num=i,
                semaphore=network.HTTP_S,
                log=log,
            )

            event = ev["event"]

            key = f"[{sport}] {event} ({TAG})"

            tvg_id, logo = leagues.get_tvg_info(sport, event)

            entry = {
                "url": url,
                "logo": logo,
                "base": BASE_URLS[sport],
                "timestamp": now.timestamp(),
                "id": tvg_id or "Live.Event.us",
                "link": link,
            }

            cached_urls[key] = entry

            if url:
                urls[key] = entry

        log.info(f"Collected and cached {len(urls)} event(s)")

    else:
        log.info("No events found")

    CACHE_FILE.write(cached_urls)
