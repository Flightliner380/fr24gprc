import secrets
import base64
import struct
from collections.abc import MutableMapping
import httpx
import asyncio
from loguru import logger
from proto.request_pb2 import (
    LiveFeedRequest,
    LiveFeedResponse
)
from google.protobuf.json_format import MessageToDict
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

DEFAULT_REQUEST = LiveFeedRequest(
    bounds=LiveFeedRequest.Bounds(
        north=28.12,
        south=12.89,
        west=96.04,
        east=129.82
    ),
    settings=LiveFeedRequest.Settings(
        sources_list=list(range(10)),
        services_list=list(range(12)),
        traffic_type=LiveFeedRequest.Settings.TrafficType.ALL,
    ),
    field_mask=LiveFeedRequest.FieldMask(
        field_name=["flight", "reg", "route", "type", "schedule"]
        # auth required: squawk, vspeed, airspace
    ),
    stats=True,
    limit=1500,
    maxage=14400,
)
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'fr24-device-id': "web-00000000000000000000000000000000",
    'x-envoy-retry-grpc-on': 'unavailable',
    'Content-Type': 'application/grpc-web+proto',
    'X-User-Agent': 'grpc-web-javascript/0.1',
    'X-Grpc-Web': '1',
    'Origin': 'https://www.flightradar24.com',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'https://www.flightradar24.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'TE': 'trailers'
}
FLIGHTINFO_SCHEMA = pa.schema([
    pa.field("flightid", pa.uint64()),
    pa.field("latitude", pa.float32()),
    pa.field("longitude", pa.float32()),
    pa.field("heading", pa.uint16()),
    pa.field("altitude", pa.int32()),
    pa.field("ground_speed", pa.int16()),
    pa.field("timestamp", pa.uint32()),
    pa.field("on_ground", pa.bool_()),
    pa.field("callsign", pa.string()),
    pa.field("source", pa.uint8()),
    pa.field("extra_info.reg", pa.string()),
    pa.field("extra_info.route.from", pa.string()),
    pa.field("extra_info.route.to", pa.string()),
    pa.field("extra_info.type", pa.string()),
    pa.field("extra_info.schedule.eta", pa.uint32()),
])
LNG_BOUNDS = [
    -180, -117, -110, -100, -95, -90, -85, -82, -79, -75, -68, -30, -2,
    1, 5, 8, 11, 15, 20, 30, 40, 60, 100, 110, 120, 140, 180
]
BOUNDS = [(LNG_BOUNDS[i], LNG_BOUNDS[i+1]) for i in range(len(LNG_BOUNDS)-1)]

# copied from: https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
def flatten(d: dict, parent_key='', sep='.'):
    items = []
    for key, value in d.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)

def generate_post_data(west, east):
    request = LiveFeedRequest()
    request.CopyFrom(DEFAULT_REQUEST)
    request.bounds.CopyFrom(LiveFeedRequest.Bounds(
        north=90,
        south=-90,
        west=west,
        east=east
    ))
    request_s = request.SerializeToString()
    post_data = b"\x00" + struct.pack("!I", len(request_s)) + request_s
    return post_data

def construct_requests(we_bounds: list[tuple[int, int]]) -> list[httpx.Request]:
    requests = []
    for west, east in we_bounds:
        data = generate_post_data(west, east)
        headers = DEFAULT_HEADERS.copy()
        headers['fr24-device-id'] = f"web-{secrets.token_urlsafe(32)}"
        requests.append(httpx.Request(
            "POST",
            "https://data-feed.flightradar24.com/fr24.feed.api.v1.Feed/LiveFeed",
            headers=headers,
            content=data
        ))
    return requests

def handle_response(data: bytes, i=0) -> LiveFeedResponse:
    # print(data[:500])
    # print(base64.b64encode(data).decode("utf-8"))
    assert len(data) and data[0] == 0
    data_len = int.from_bytes(data[1:5], byteorder="big")
    # print("len", data[1:5], data_len)
    # print(base64.b64encode(data[5:5+data_len]).decode("utf-8"))
    lfr = LiveFeedResponse()
    lfr.ParseFromString(data[5:5+data_len])
    return lfr, i

async def do_request(client: httpx.AsyncClient, request: httpx.Request, i=0):
    response = await client.send(request)
    return handle_response(response.content, i)

async def download_full():
    requests = construct_requests(we_bounds=BOUNDS)
    async with httpx.AsyncClient() as client:
        data = await asyncio.gather(*[do_request(client, request, i) for i, request in enumerate(requests)])
        all_flights = []
        for d, i in data:
            d2 = MessageToDict(d, including_default_value_fields=True, use_integers_for_enums=True, preserving_proto_field_name=True)["flights_list"]
            for d3 in d2:
                all_flights.append(flatten(d3))
            logger.debug(f"{BOUNDS[i]}: {len(d2)} flights")
        tbl = pa.Table.from_pylist(all_flights, schema=FLIGHTINFO_SCHEMA)
        pq.write_table(tbl, "tmp/test.parquet")
        csv.write_csv(tbl, "tmp/test.csv")

async def download_simple():
    req = construct_requests(we_bounds=[(1, 5)])[0] # filter all flights between lngs 1-5
    # print(req.__dict__)
    # print(base64.b64encode(req._content).decode("utf-8"))
    async with httpx.AsyncClient() as client:
        data = await do_request(client, req)
        assert len(data) > 0
        print(data)

if __name__ == '__main__':
    asyncio.run(download_simple())
    # asyncio.run(download_full())