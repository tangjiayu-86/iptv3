import asyncio
import re
from itertools import chain
from urllib.parse import urljoin

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "FUTBOLX"

CACHE_FILE = Cache(TAG, exp=19_800)

BASE_URL = "https://futbol-x.xyz"

SPORT_URLS = [
    urljoin(BASE_URL, f"api/{sport}.json")
    for sport in [
        # "basketball",
        # "darts",
        "fights",
        "football",
        # "golf",
        "mlb",
        "motorsports",
        "nfl",
        # "nhl",
        # "others",
        # "rugby",
        # "tennis",
        "wrestling",
    ]
]


async def get_events() -> dict[str, dict[str, str | float]]:
    events = {}

    tasks = [network.request(url, log=log) for url in SPORT_URLS]

    results = await asyncio.gather(*tasks)

    if not (
        api_data := [
            *chain.from_iterable(r.json().get("streams", {}) for r in results if r)
        ]
    ):
        return events

    now = Time.clean(Time.now())

    ptrn = re.compile(r"^http.*\.m3u8$", re.I)

    for event in api_data:
        if not (streams := event.get("streams")):
            continue

        for event_info in streams:
            if not all(
                values := [
                    event_info.get(k)
                    for k in (
                        "name",
                        "tag",
                        "starts_at",
                        "streams",
                    )
                ]
            ):
                continue

            event_name, sport, event_time, event_streams = values

            if not (event_name and sport and event_time and event_streams):
                continue

            sport = sport.upper() if len(sport) == 3 else sport

            event_dt = Time.from_str(event_time, timezone="MSK")

            if event_dt.date() != now.date():
                continue

            for i, stream_info in enumerate(event_streams, start=1):
                if not (url := stream_info.get("url")):
                    continue

                elif not ptrn.search(url):
                    continue

                key = f"[{sport}] {event_name} {i} ({TAG})"

                tvg_id, logo = leagues.get_tvg_info(sport, event_name)

                events[key] = {
                    "url": url,
                    "logo": logo,
                    "base": BASE_URL,
                    "timestamp": now.timestamp(),
                    "id": tvg_id or "Live.Event.us",
                }

    return events


async def scrape() -> None:
    if cached := CACHE_FILE.load():
        urls.update(cached)

        log.info(f"Loaded {len(urls)} event(s) from cache")

        return

    log.info(f'Scraping from "{BASE_URL}"')

    urls.update(await get_events() or {})

    log.info(f"Collected and cached {len(urls)} new event(s)")

    CACHE_FILE.write(urls)
