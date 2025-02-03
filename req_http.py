import asyncio

import requests

from logger_utils import logger

# A few handy JSON types
JSON = int | str | float | bool | None | dict[str, "JSON"] | list["JSON"]
JSONObject = dict[str, JSON]
JSONList = list[JSON]


def http_get_sync(url: str) -> JSONObject:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


async def http_get(url: str) -> JSONObject | None:
    try:
        return await asyncio.to_thread(http_get_sync, url)
    except requests.exceptions.HTTPError as e:
        logger.error(e)
        if e.response.status_code == 414:
            print(
                "City boundary extremely complex! "
                + "GeoJson expression too long to be processed. "
                + "Try with another city!"
            )
    except requests.exceptions.ConnectionError as e:
        logger.error(e)
        if len(url) > 8000:
            print(
                "City boundary extremely complex! "
                + "GeoJson expression too long to be processed. "
                + "Try with another city!"
            )
        else:
            print("Connection error")
