from datetime import datetime
from typing import Optional

import typer

from spellbook import cli_export, cli_get

app = typer.Typer()
app.add_typer(cli_get.app, name="get")


@app.command()
def export(
    feature: str = "",
    snapshot_date: Optional[datetime] = None,
    output_file: str = "",
    repo_config: str = "",
):
    typer.echo(f"{feature}, {snapshot_date}, {output_file}, {repo_config}")


if __name__ == "__main__":
    app()
