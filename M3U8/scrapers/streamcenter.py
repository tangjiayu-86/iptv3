import re
from collections import defaultdict
from functools import partial
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "STRMCNTR"

CACHE_FILE = Cache(TAG, exp=28_800)

API_URL = "https://backend.streamcenter.live/api/Parties"

BASE_URL = "https://streams.center"

CATEGORIES = {
    # 4: "Basketball",
    9: "Football",
    13: "Baseball",
    # 14: "American Football",
    15: "Motor Sport",
    # 16: "Hockey",
    17: "Fight MMA",
    18: "Boxing",
    20: "WWE",
    21: "Tennis",
}


async def process_event(url: str, url_num: int) -> str | None:
    if not (html_data := await network.request(url, log=log)):
        log.warning(f"URL {url_num}) Failed to load url.")
        return

    soup = HTMLParser(html_data.content)

    iframe = soup.css_first("iframe")

    if not iframe or not (src := iframe.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe element found.")
        return

    if not (
        iframe_src_data := await network.request(
            network.ensure_https(src),
            headers={"Referer": url},
            log=log,
        )
    ):
        log.warning(f"URL {url_num}) Failed to load iframe source.")
        return

    pattern = re.compile(r'input:\s+"([^"]*)"', re.I)

    if not (match := pattern.search(iframe_src_data.text)):
        log.warning(f"URL {url_num}) No encrypted URL found.")
        return

    if not (
        decrypted := await network.client.post(
            urljoin(BASE_URL, "embed/decrypt.php"),
            data={"input": match[1]},
        )
    ):
        log.warning(f"URL {url_num}) Failed to decrypt URL.")
        return

    log.info(f"URL {url_num}) Captured M3U8")

    return decrypted.text.split("?")[0]


async def get_events() -> list[dict[str, str]]:
    events = []

    if not (
        r := await network.request(
            API_URL,
            params={"pageNumber": 1, "pageSize": 500},
            log=log,
        )
    ):
        return events

    now = Time.clean(Time.now())

    api_data: list[dict] = r.json()

    counter = defaultdict(int)

    for stream_group in api_data:
        if not all(
            values := [
                stream_group.get(x)
                for x in (
                    "categoryId",
                    "gameName",
                    "videoUrl",
                    "beginPartie",
                )
            ]
        ):
            continue

        category_id, title, iframes, event_time = values

        if not (sport := CATEGORIES.get(category_id)):
            continue

        event_dt = Time.from_str(event_time, timezone="CET")

        if event_dt.date() != now.date():
            continue

        stream_urls: dict[str, str] = {
            url: lang
            for entry in iframes.split(";")
            for url, lang in [entry.split("<")]
        }

        for url, lang in stream_urls.items():
            counter[name := f"{title} | {lang}"] += 1

            events.append(
                {
                    "sport": sport,
                    "event": f"{name} {counter[name]}",
                    "link": url,
                    "timestamp": now.timestamp(),
                }
            )

    return events


async def scrape() -> None:
    if cached_urls := CACHE_FILE.load():
        urls.update({k: v for k, v in cached_urls.items() if v["url"]})

        log.info(f"Loaded {len(urls)} event(s) from cache")

        return

    log.info(f'Scraping from "{BASE_URL}"')

    if events := await get_events():
        log.info(f"Processing {len(events)} URL(s)")

        for i, ev in enumerate(events, start=1):
            handler = partial(
                process_event,
                url=(link := ev["link"]),
                url_num=i,
            )

            url = await network.safe_process(
                handler,
                url_num=i,
                semaphore=network.HTTP_S,
                log=log,
            )

            sport, event, ts = (
                ev["sport"],
                ev["event"],
                ev["timestamp"],
            )

            key = f"[{sport}] {event} ({TAG})"

            tvg_id, logo = leagues.get_tvg_info(sport, event)

            entry = {
                "url": url,
                "logo": logo,
                "base": BASE_URL,
                "timestamp": ts,
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
