from __future__ import annotations

import httpx
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import ScrollableContainer
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Static,
)

import pandas as pd
from fr24.authentication import login
from fr24.history import airport_list, flight_list
from fr24.tui.formatters import Aircraft, Airport, Time
from fr24.tui.widgets import AircraftWidget, AirportWidget, FlightWidget
from fr24.types.fr24 import (
    AirportList,
    Authentication,
    FlightList,
    FlightListItem,
)


class SearchBlock(Static):
    def compose(self) -> ComposeResult:
        yield Label("date")
        self.date_input = Input(id="date")
        self.date_input.value = f"{pd.Timestamp('now'):%d %b %y}"
        yield self.date_input
        yield AircraftWidget(name="aircraft")
        yield FlightWidget(name="number")
        yield AirportWidget(id="departure", name="origin")
        yield AirportWidget(id="arrival", name="destination")


# -- Application --


class FR24(App[None]):
    CSS_PATH = "style.tcss"
    BINDINGS = [  # noqa: RUF012
        ("q", "quit", "Quit"),
        ("l", "login", "Log in"),
        ("r", "refresh", "Refresh"),  # TODO
        ("s", "search", "Search"),
        ("c", "clear", "Clear"),
        Binding("escape", "escape", show=False),
    ]

    def compose(self) -> ComposeResult:
        self.auth: Authentication | None = None
        self.client = httpx.AsyncClient()
        self.search_visible = True
        yield Header()
        yield Footer()
        yield SearchBlock()
        yield ScrollableContainer(DataTable())

    def on_mount(self) -> None:
        self.title = "FlightRadar24"
        table = self.query_one(DataTable)
        table.add_columns(
            "date",
            "number",
            "callsign",
            "aircraft",
            "from",
            "to",
            "STD",
            "ATD",
            "STA",
            "status",
            "flightid",
        )

    def action_search(self) -> None:
        self.search_visible = not self.search_visible
        if self.search_visible:
            self.query_one(SearchBlock).remove_class("hidden")
        else:
            self.query_one(SearchBlock).add_class("hidden")
            self.query_one(DataTable).focus()

    def action_clear(self) -> None:
        for widget in self.query(Input):
            if widget.id != "date":
                widget.value = ""

    async def action_escape(self) -> None:
        if not self.search_visible:
            await self.action_quit()

        self.search_visible = False
        self.query_one(SearchBlock).add_class("hidden")
        self.query_one(DataTable).focus()

    async def action_login(self) -> None:
        self.auth = await login(self.client)
        if self.auth is not None:
            self.sub_title = f"(authenticated: {self.auth['user']['identity']})"
            self.query_one(Header).add_class("authenticated")
            self.query_one(Footer).add_class("authenticated")

    @on(Input.Submitted)
    async def action_refresh(self) -> None:
        ts_str = self.query_one("#date", Input).value
        ts = pd.Timestamp(ts_str if ts_str else "now", tz="utc")

        aircraft_widget = self.query_one(AircraftWidget)
        if aircraft := aircraft_widget.aircraft_id:
            await self.lookup_aircraft(aircraft, ts=ts)
            return
        number_widget = self.query_one(FlightWidget)
        if number := number_widget.number:
            await self.lookup_number(number, ts=ts)
            return
        departure_widget = self.query_one("#departure", AirportWidget)
        if departure := departure_widget.airport_id:
            await self.lookup_departure(departure, ts=ts)
            return
        arrival_widget = self.query_one("#arrival", AirportWidget)
        if arrival := arrival_widget.airport_id:
            await self.lookup_arrival(arrival, ts=ts)
            return

    async def lookup_aircraft(self, value: str, ts: str) -> None:
        results: FlightList = await flight_list(
            self.client, reg=value, limit=100, timestamp=ts, auth=self.auth
        )
        self.update_table(results["result"]["response"].get("data", None))

    async def lookup_number(self, value: str, ts: str) -> None:
        results: FlightList = await flight_list(
            self.client, flight=value, limit=100, timestamp=ts, auth=self.auth
        )
        self.update_table(results["result"]["response"].get("data", None))

    async def lookup_arrival(self, value: str, ts: str) -> None:
        results: AirportList = await airport_list(
            self.client,
            airport=value,
            mode="arrivals",
            limit=100,
            timestamp=ts,
            auth=self.auth,
        )
        s = results["result"]["response"]["airport"]["pluginData"]["schedule"]
        self.update_table(
            [  # TODO add airport info from
                elt["flight"]  # type: ignore
                for elt in s["arrivals"].get("data", [])
            ]
        )

    async def lookup_departure(self, value: str, ts: str) -> None:
        results: AirportList = await airport_list(
            self.client,
            airport=value,
            mode="departures",
            limit=100,
            timestamp=ts,
            auth=self.auth,
        )
        s = results["result"]["response"]["airport"]["pluginData"]["schedule"]
        self.update_table(
            [  # TODO add airport info from
                elt["flight"]  # type: ignore
                for elt in s["departures"].get("data", [])
            ]
        )

    def update_table(self, data: None | list[FlightListItem]) -> None:
        table = self.query_one(DataTable)
        table.clear()
        if data is None:
            return
        table.add_rows(
            [
                (
                    f'{Time(entry["time"]["scheduled"]["departure"]):%d %b %y}',
                    entry["identification"]["number"]["default"],
                    entry["identification"]["callsign"],
                    f'{Aircraft(entry["aircraft"]):%r (%c)}',
                    f'{Airport(entry["airport"]["origin"]):%y (%o)}',
                    f'{Airport(entry["airport"]["destination"]):%y (%o)}',
                    f'{Time(entry["time"]["scheduled"]["departure"]):%H:%MZ}',
                    f'{Time(entry["time"]["real"]["departure"]):%H:%MZ}',
                    f'{Time(entry["time"]["scheduled"]["arrival"]):%H:%MZ}',
                    entry["status"]["text"],
                    entry["identification"]["id"],
                )
                for entry in data
            ]
        )


def main() -> None:
    app = FR24()
    app.run()


if __name__ == "__main__":
    main()
