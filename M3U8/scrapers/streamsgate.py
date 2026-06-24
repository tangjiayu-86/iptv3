import asyncio
import json
import re
from functools import partial
from itertools import chain
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "STRMSGATE"

CACHE_FILE = Cache(TAG, exp=19_800)

BASE_URL = "https://streamsgates.io"

SPORT_URLS = [
    urljoin(BASE_URL, f"data/{sport}.json")
    for sport in [
        # "cfb",
        "mlb",
        # "nba",
        # "nfl",
        # "nhl",
        "soccer",
        "ufc",
    ]
]


def get_event(t1: str, t2: str) -> str:
    match t1:
        case "RED ZONE":
            return "NFL RedZone"

        case "TBD":
            return "TBD"

        case _:
            return f"{t1.strip()} vs {t2.strip()}"


async def process_event(url: str, url_num: int) -> tuple[str | None, str | None]:
    nones = None, None

    if not (event_data := await network.request(url, log=log)):
        log.warning(f"URL {url_num}) Failed to load url.")
        return nones

    soup = HTMLParser(event_data.content)

    ifr = soup.css_first("iframe")

    if not ifr or not (src := ifr.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe element found.")
        return nones

    ifr_src = network.ensure_https(src)

    if not (
        ifr_src_data := await network.request(
            ifr_src,
            headers={"Referer": url},
            log=log,
        )
    ):
        log.warning(f"URL {url_num}) Failed to load iframe source.")
        return nones

    valid_m3u8 = re.compile(
        r"(file|source|currentStreamUrl)\s*(:|=)\s+(\'|\")([^\"]*)(\'|\")",
        re.I,
    )

    if not (match := valid_m3u8.search(ifr_src_data.text)):
        log.warning(f"URL {url_num}) No source found.")
        return nones

    log.info(f"URL {url_num}) Captured M3U8")

    return json.loads(f'"{match[4]}"'), ifr_src


async def get_events() -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    tasks = [network.request(url, log=log) for url in SPORT_URLS]

    results = await asyncio.gather(*tasks)

    events = []

    if not (api_data := [*chain.from_iterable(r.json() for r in results if r)]):
        return events

    for stream_group in api_data:
        if not all(
            values := [
                stream_group.get(x)
                for x in (
                    "time",
                    "league",
                    "away",
                    "home",
                )
            ]
        ):
            continue

        date, sport, t1, t2 = values

        event_dt = Time.from_str(date, timezone="UTC")

        if event_dt.date() != now.date():
            continue

        if not (streams := stream_group.get("streams")):
            continue

        elif not (
            valid_streams := sorted(
                [
                    *filter(
                        lambda x: (lth := len(x.get("lang"))) == 0 or lth > 2,
                        streams,
                    )
                ],
                key=lambda x: len(x.get("lang")),
                reverse=True,
            )
        ):
            continue

        url = valid_streams[0]["url"]

        event = get_event(t1, t2)

        events.append(
            {
                "sport": sport,
                "event": event,
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
        log.info(f"Processing {len(events)} new URL(s)")

        for i, ev in enumerate(events, start=1):
            handler = partial(
                process_event,
                url=(link := ev["link"]),
                url_num=i,
            )

            url, iframe = await network.safe_process(
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
                "base": iframe,
                "timestamp": ts,
                "id": tvg_id or "Live.Event.us",
                "link": link,
            }

            cached_urls[key] = entry

            if url:
                entry["url"] = url.split("?st")[0]

                urls[key] = entry

        log.info(f"Collected and cached {len(urls)} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
