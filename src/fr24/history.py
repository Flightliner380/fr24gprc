from __future__ import annotations

import secrets
from datetime import datetime
from typing import Literal

import httpx

import pandas as pd

from .types.fr24 import (
    AirportList,
    AirportRequest,
    Authentication,
    FlightList,
    FlightListRequest,
    Playback,
    PlaybackRequest,
)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) "
    "Gecko/20100101 Firefox/116.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;"
    "q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://www.flightradar24.com",
    "Connection": "keep-alive",
    "Referer": "https://www.flightradar24.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "TE": "trailers",
}


async def flight_list(
    client: httpx.AsyncClient,
    *,
    reg: None | str = None,
    flight: None | str = None,
    page: int = 1,
    limit: int = 10,
    timestamp: int | datetime | pd.Timestamp | str | None = "now",
    auth: None | Authentication = None,
) -> FlightList:
    if isinstance(timestamp, (str, datetime)):
        timestamp = pd.Timestamp(timestamp)
    if isinstance(timestamp, pd.Timestamp):
        timestamp = int(timestamp.timestamp())

    if reg is not None:
        key, value = "reg", reg
    elif flight is not None:
        key, value = "flight", flight
    else:
        msg = "One named arguments among `reg` and `flight` is expected"
        raise TypeError(msg)

    device = f"web-{secrets.token_urlsafe(32)}"
    headers = DEFAULT_HEADERS.copy()
    headers["fr24-device-id"] = device
    params: FlightListRequest = {
        "query": value,
        "timestamp": timestamp,
        "fetchBy": key,
        "page": page,
        "limit": limit,
    }

    if timestamp is None:
        del params["timestamp"]

    if auth is not None and auth["userData"]["subscriptionKey"] is not None:
        params["token"] = auth["userData"]["subscriptionKey"]
    else:
        params["device"] = device

    request = httpx.Request(
        "GET",
        "https://api.flightradar24.com/common/v1/flight/list.json",
        headers=DEFAULT_HEADERS,
        params=params,  # type: ignore
    )

    response = await client.send(request)
    response.raise_for_status()
    return response.json()  # type: ignore


async def airport_list(
    client: httpx.AsyncClient,
    airport: str,
    mode: Literal["arrivals"] | Literal["departures"],
    page: int = 1,
    limit: int = 10,
    timestamp: int | datetime | pd.Timestamp | str | None = "now",
    auth: None | Authentication = None,
) -> AirportList:
    if isinstance(timestamp, (str, datetime)):
        timestamp = pd.Timestamp(timestamp)
    if isinstance(timestamp, pd.Timestamp):
        timestamp = int(timestamp.timestamp())

    device = f"web-{secrets.token_urlsafe(32)}"
    headers = DEFAULT_HEADERS.copy()
    headers["fr24-device-id"] = device

    params: AirportRequest = {
        "code": airport,
        "plugin[]": ["schedule"],
        "plugin-setting[schedule][mode]": mode,
        "plugin-setting[schedule][timestamp]": timestamp,
        "page": page,
        "limit": limit,
    }

    if timestamp is None:
        del params["plugin-setting[schedule][timestamp]"]

    if auth is not None and auth["userData"]["subscriptionKey"] is not None:
        params["token"] = auth["userData"]["subscriptionKey"]
    else:
        params["device"] = device

    request = httpx.Request(
        "GET",
        "https://api.flightradar24.com/common/v1/airport.json",
        headers=DEFAULT_HEADERS,
        params=params,  # type: ignore
    )

    response = await client.send(request)
    response.raise_for_status()
    return response.json()  # type: ignore


async def playback(
    client: httpx.AsyncClient,
    flight_id: int | str,
    timestamp: int | str | datetime | pd.Timestamp,
    auth: None | Authentication = None,
) -> Playback:
    # NOTE: timestamp must match ATOD
    if isinstance(timestamp, (str, datetime)):
        timestamp = pd.Timestamp(timestamp, tz="utc")
    if isinstance(timestamp, pd.Timestamp):
        timestamp = int(timestamp.timestamp())
    if not isinstance(flight_id, str):
        flight_id = f"{flight_id:x}"

    device = f"web-{secrets.token_urlsafe(32)}"
    headers = DEFAULT_HEADERS.copy()
    headers["fr24-device-id"] = device
    params = PlaybackRequest(
        flightId=flight_id,
        timestamp=timestamp,
    )
    if auth is not None and auth["userData"]["subscriptionKey"] is not None:
        params["token"] = auth["userData"]["subscriptionKey"]
    else:
        params["device"] = device

    request = httpx.Request(
        "GET",
        "https://api.flightradar24.com/common/v1/flight-playback.json",
        headers=DEFAULT_HEADERS,
        params=params,  # type: ignore[arg-type]
    )

    response = await client.send(request)
    response.raise_for_status()
    return response.json()  # type: ignore


def playback_df(result: Playback) -> pd.DataFrame:
    flight = result["result"]["response"]["data"]["flight"]
    df = pd.DataFrame.from_records(
        {
            **point,
            "altitude": point["altitude"]["feet"],
            "speed": point["speed"]["kts"],
            "verticalSpeed": point["verticalSpeed"]["fpm"],
        }
        for point in flight["track"]
    ).rename(columns=dict(speed="groundspeed", verticalSpeed="vertical_rate"))
    ems = [
        point["ems"] for point in flight["track"] if point["ems"] is not None
    ]
    if len(ems) > 0:
        ems_df = pd.json_normalize(ems).rename(columns=dict(ts="timestamp"))
        df = pd.concat([df, ems_df]).sort_values("timestamp")

    df = df.eval(
        """
timestamp = @pd.to_datetime(timestamp, unit='s', utc=True)
callsign = @flight["identification"]["callsign"]
number = @flight["identification"]["number"]["default"]
icao24 = @flight["aircraft"]["identification"]["modes"].lower()
registration = @flight["aircraft"]["identification"]["registration"]
flight_id = @flight['identification']["id"]
"""
    )
    return df.drop(columns=["ems"])


def flight_list_df(result: FlightList) -> pd.DataFrame:
    list_ = result["result"]["response"]["data"]
    # print(f'{result["result"]["response"]["page"]["more"]=}')
    df = pd.DataFrame.from_records(
        {
            "flight_id": entry["identification"]["id"],
            "number": entry["identification"]["number"]["default"],
            "callsign": entry["identification"]["callsign"],
            "icao24": entry["aircraft"]["hex"],
            "registration": entry["aircraft"]["registration"],
            "typecode": entry["aircraft"]["model"]["code"],
            "origin": entry["airport"]["origin"]["code"]["icao"],
            "destination": entry["airport"]["destination"]["code"]["icao"],
            "status": entry["status"]["text"],
            "STOD": entry["time"]["scheduled"]["departure"],
            "ETOD": entry["time"]["estimated"]["departure"],
            "ATOD": entry["time"]["real"]["departure"],
            "STOA": entry["time"]["scheduled"]["arrival"],
            "ETOA": entry["time"]["estimated"]["arrival"],
            "ATOA": entry["time"]["real"]["arrival"],
        }
        for entry in list_
    )
    return df.eval(
        """
icao24 = icao24.str.lower()
STOD = @pd.to_datetime(STOD, unit="s", utc=True)
ETOD = @pd.to_datetime(ETOD, unit="s", utc=True)
ATOD = @pd.to_datetime(ATOD, unit="s", utc=True)
STOA = @pd.to_datetime(STOA, unit="s", utc=True)
ETOA = @pd.to_datetime(ETOA, unit="s", utc=True)
ATOA = @pd.to_datetime(ATOA, unit="s", utc=True)
"""
    )
