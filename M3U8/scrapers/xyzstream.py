from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "XYZSTRM"

CACHE_FILE = Cache(TAG, exp=28_800)

BASE_URL = "https://xyzstreams.shop/"

API_URL = "https://api.streamxyz.shop:2053/api/scoreboard"


async def get_events() -> dict[str, dict[str, str | float]]:
    events = {}

    if not (
        r := await network.request(
            API_URL,
            headers={"Referer": BASE_URL},
            log=log,
        )
    ):
        return events

    now = Time.clean(Time.now())

    api_data: list[dict[str, str | dict]] = r.json()

    sport = "Live Event"

    for event_info in api_data:
        away_team: str = event_info.get("away", {}).get("name")
        home_team: str = event_info.get("home", {}).get("name")
        event_date: str = event_info.get("gameDate")

        if not (event_date and away_team and home_team):
            continue

        event_dt = Time.fromisoformat(event_date)

        if event_dt.date() != now.date():
            continue

        if not (feeds := event_info.get("feeds")):
            continue

        event_name = f"{away_team} vs {home_team}"

        for i, feed in enumerate(feeds.values(), start=1):
            key = f"[{sport}] {event_name} {i} ({TAG})"

            tvg_id, logo = leagues.get_tvg_info(sport, event_name)

            events[key] = {
                "url": feed,
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
