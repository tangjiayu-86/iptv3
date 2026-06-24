import asyncio
import json
from functools import partial
from typing import Any
from urllib.parse import urlencode, urljoin

from playwright.async_api import Browser, Page

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "WATCHFTY"

CACHE_FILE = Cache(TAG, exp=10_800)

BASE_DOMAIN = "watchfooty.ru"

API_URL, BASE_URL = f"https://api.{BASE_DOMAIN}", f"https://www.{BASE_DOMAIN}"


def build_wfty_url(live: bool, event_id: str = None) -> str:
    url = urljoin(
        API_URL,
        (
            "_internal/trpc/sports.getSportsLiveMatchesCount,sports.getPopularMatches,sports.getPopularLiveMatches"
            if live
            else "_internal/trpc/sports.getSportsLiveMatchesCount,sports.getMatchById"
        ),
    )

    input_data = {
        "0": {
            "json": {
                "start": (now := Time.now()).isoformat(),
                "end": now.delta(days=1).isoformat(),
            }
        },
    } | (
        {
            "1": {
                "json": None,
                "meta": {"values": ["undefined"]},
            },
            "2": {
                "json": None,
                "meta": {"values": ["undefined"]},
            },
        }
        if live
        else {
            "1": {
                "json": {
                    "id": event_id,
                    "withoutAdditionalInfo": True,
                    "withoutLinks": False,
                }
            }
        }
    )

    params = {
        "batch": "1",
        "input": json.dumps(input_data, separators=(",", ":")),
    }

    return f"{url}?{urlencode(params)}"


async def pre_process(url: str, url_num: int) -> str | None:
    if not (event_data := await network.request(url, log=log)):
        return

    api_data: dict = (
        (event_data.json() or [{}])[-1].get("result", {}).get("data", {}).get("json")
    )

    if not api_data:
        log.warning(f"URL {url_num}) No API data found.")
        return

    if not (links := api_data.get("fixtureData", {}).get("links")):
        log.warning(f"URL {url_num}) No stream links found.")
        return

    quality_links: list[dict] = sorted(
        [link for link in links if (wld := link.get("wld")) and "e" not in wld],
        key=lambda x: x.get("viewerCount") or -1,
        reverse=True,
    )

    if not quality_links:
        log.warning(f"URL {url_num}) No valid quality link found.")
        return

    link_data = quality_links[0]

    embed_path = (
        link_data["gi"],
        link_data["t"],
        link_data["wld"]["cn"],
        link_data["wld"]["sn"],
    )

    return f"https://sportsembed.su/embed/{'/'.join(embed_path)}?player=clappr&autoplay=true"


async def process_event(
    url: str,
    url_num: int,
    page: Page,
) -> tuple[str | None, str | None]:

    nones = None, None

    captured: list[str] = []

    got_one = asyncio.Event()

    handler = partial(
        network.capture_req,
        captured=captured,
        got_one=got_one,
    )

    page.on("request", handler)

    if not (iframe_url := await pre_process(url, url_num)):
        return nones

    try:
        resp = await page.goto(
            iframe_url,
            wait_until="domcontentloaded",
            timeout=6_000,
        )

        if not resp or resp.status != 200:
            log.warning(
                f"URL {url_num}) Status Code: {resp.status if resp else 'None'}"
            )
            return nones

        wait_task = asyncio.create_task(got_one.wait())

        try:
            await asyncio.wait_for(wait_task, timeout=6)
        except TimeoutError:
            log.warning(f"URL {url_num}) Timed out waiting for M3U8.")
            return nones

        finally:
            if not wait_task.done():
                wait_task.cancel()

                try:
                    await wait_task
                except asyncio.CancelledError:
                    pass

        if captured:
            log.info(f"URL {url_num}) Captured M3U8")
            return captured[0], iframe_url

    except Exception as e:
        log.warning(f"URL {url_num}) {e}")
        return nones

    finally:
        page.remove_listener("request", handler)


async def get_events(cached_keys: list[str]) -> list[dict[str, str]]:
    events = []

    live_url = build_wfty_url(live=True)

    if not (live_data := await network.request(live_url, log=log)):
        return events

    api_data: list[dict[str, Any]] = (
        (live_data.json() or [{}])[-1].get("result", {}).get("data", {}).get("json")
    )

    if not api_data:
        return events

    for link in api_data:
        if not link.get("viewerCount"):
            continue

        event_id: str = link["id"]

        event_league: str = link["league"]

        event_name: str = link["title"]

        if f"[{event_league}] {event_name} ({TAG})" in cached_keys:
            continue

        events.append(
            {
                "sport": event_league,
                "event": event_name,
                "link": build_wfty_url(live=False, event_id=event_id),
            }
        )

    return events


async def scrape(browser: Browser) -> None:
    cached_urls = CACHE_FILE.load()

    valid_urls = {k: v for k, v in cached_urls.items() if v["url"]}

    valid_count = cached_count = len(valid_urls)

    urls.update(valid_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    log.info(f'Scraping from "{BASE_URL}"')

    if events := await get_events(cached_urls.keys()):
        log.info(f"Processing {len(events)} new URL(s)")

        now = Time.clean(Time.now())

        async with network.event_context(browser, stealth=False) as context:
            for i, ev in enumerate(events, start=1):
                async with network.event_page(context) as page:
                    handler = partial(
                        process_event,
                        url=(link := ev["link"]),
                        url_num=i,
                        page=page,
                    )

                    url, iframe = await network.safe_process(
                        handler,
                        url_num=i,
                        semaphore=network.PW_S,
                        log=log,
                        timeout=20,
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
