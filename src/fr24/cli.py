import asyncio
from datetime import datetime
from typing import Annotated, Optional

# import pyarrow as pa
import rich
import typer
from appdirs import user_cache_dir, user_config_dir

# from pyarrow.parquet import ParquetFile
from .core import FR24
from .tui.tui import main as tui_main

app = typer.Typer()


@app.command()
def dirs() -> None:
    """Shows relevant directories"""
    rich.print(f"Config: {user_config_dir('fr24')}")
    rich.print(f" Cache: {user_cache_dir('fr24')}")


@app.command()
def tui() -> None:
    """Starts the TUI"""

    tui_main()


app_auth = typer.Typer()
app.add_typer(app_auth, name="auth")


@app_auth.command()
def show() -> None:
    """Shows authentication status"""

    async def show_() -> None:
        async with FR24() as fr24:
            if fr24.http.auth is None:
                rich.print(
                    "[red1]"
                    "You are not authenticated.\n"
                    "Provide credentials in environment variables: either \n"
                    "- fr24_username + fr24_password or \n"
                    "- fr24_subscription_key + fr24_token (optional)"
                    "[/red1]"
                )
            else:
                rich.print("[bold green]Login successful[/bold green]")
                rich.print(fr24.http.auth)

    asyncio.run(show_())


app_feed = typer.Typer()
app.add_typer(app_feed, name="feed")


@app_feed.command()
def live() -> None:
    """Downloads current live feed"""

    async def live_() -> None:
        async with FR24() as fr24:
            await fr24.cache_livefeed()
            rich.print("[red1]This command is under maintenance.[/red1]")
            # rich.print(f"[bold green]Success[/bold green]: {fp}")

    asyncio.run(live_())


@app_feed.command()
def playback(
    timestamp: Annotated[
        Optional[int], typer.Option(help="Start unix timestamp (s, UTC)")
    ] = None,
    time: Annotated[
        Optional[datetime], typer.Option(help="Start datetime (UTC)")
    ] = None,
    duration: Annotated[int, typer.Option(help="Prefetch (seconds)")] = 7,
    hfreq: int = 0,
) -> None:
    """Downloads current playback feed"""
    if time is None and timestamp is None:
        raise typer.BadParameter("Either time or timestamp must be set")
    if time is not None and timestamp is not None:
        raise typer.BadParameter("Only one of time and timestamp can be set")
    if time is not None:
        timestamp = int(time.timestamp())

    async def playback_() -> None:
        async with FR24() as fr24:
            await fr24.cache_livefeed_playback_world_insert(
                timestamp,  # type: ignore[arg-type]
                duration,
                hfreq,
            )
            rich.print("[red1]This command is under maintenance.[/red1]")
            # rich.print(f"[bold green]Success[/bold green]: {fp}")
            # pf = ParquetFile(fp)
            # num_rows = pf.metadata.num_rows if pf.metadata is not None else 0
            # rich.print("rows: ", num_rows)
            # rich.print(
            #     pa.Table.from_batches(
            #         [next(pf.iter_batches(batch_size=5))]
            #     ).to_pandas()
            # )

    asyncio.run(playback_())


if __name__ == "__main__":
    app()
