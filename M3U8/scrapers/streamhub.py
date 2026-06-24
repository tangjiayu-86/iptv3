from functools import partial
from urllib.parse import parse_qsl, urljoin, urlsplit

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "STRMHUB"

CACHE_FILE = Cache(TAG, exp=10_800)

BASE_URL = "https://streamhub.pro"


async def process_event(url: str, url_num: int) -> tuple[str | None, str | None]:
    nones = None, None

    if not (event_data := await network.request(url, log=log)):
        log.warning(f"URL {url_num}) Failed to load url.")
        return nones

    soup_1 = HTMLParser(event_data.content)

    ifr_1 = soup_1.css_first("iframe#playerIframe")

    if not ifr_1 or not (ifr_1_src := ifr_1.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe element/src found. (IFR1)")
        return nones

    elif not (ifr_1_src_data := await network.request(ifr_1_src, log=log)):
        log.warning(f"URL {url_num}) Failed to load iframe source. (IFR1)")
        return nones

    soup_2 = HTMLParser(ifr_1_src_data.content)

    ifr_2 = soup_2.css_first("iframe")

    if not ifr_2 or not (ifr_2_src := ifr_2.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe element/src found. (IFR2)")
        return nones

    params = dict(parse_qsl(urlsplit(ifr_2_src).query))

    if not (stream_key := params.get("stream")):
        log.warning(f"URL {url_num}) No stream key found.")
        return nones

    log.info(f"URL {url_num}) Captured M3U8")

    return f"https://obstreamx.click/live/{stream_key}.m3u8", ifr_2_src


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    events = []

    if not (html_data := await network.request(BASE_URL, log=log)):
        return events

    soup = HTMLParser(html_data.content)

    for card in soup.css(".live-card"):
        if not (href := card.attributes.get("href")):
            continue

        elif not (team_elems := card.css(".live-team-name")):
            continue

        sport = "".join(
            x for x in card.css_first(".live-league").text(strip=True) if x.isascii()
        ).lstrip()

        event_name = (
            "".join(x.text(strip=True) for x in team_elems)
            if len(team_elems) == 1
            else " vs ".join(x.text(strip=True) for x in team_elems)
        )

        if f"[{sport}] {event_name} ({TAG})" in cached_keys:
            continue

        events.append(
            {
                "sport": sport,
                "event": event_name,
                "link": urljoin(f"{html_data.url}", href),
            }
        )

    return events


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    valid_urls = {k: v for k, v in cached_urls.items() if v["url"]}

    valid_count = cached_count = len(valid_urls)

    urls.update(valid_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info(f'Scraping from "{BASE_URL}"')

    if events := await get_events(cached_urls.keys()):
        log.info(f"Processing {len(events)} new URL(s)")

        now = Time.clean(Time.now())

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

            sport, event = ev["sport"], ev["event"]

            key = f"[{sport}] {event} ({TAG})"

            tvg_id, logo = leagues.get_tvg_info(sport, event)

            entry = {
                "url": url,
                "logo": logo,
                "base": iframe,
                "timestamp": now.timestamp(),
                "id": tvg_id or "Live.Event.us",
                "link": link,
            }

            cached_urls[key] = entry

            if url:
                valid_count += 1

                urls[key] = entry

        log.info(f"Collected and cached {valid_count - cached_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
