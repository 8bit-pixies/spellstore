from datetime import datetime
from typing import Optional

import pandas as pd
import typer

from spellbook import cli_get
from spellbook.base import RepoConfig
from spellbook.feature_store import FeatureStore

app = typer.Typer()
app.add_typer(cli_get.app, name="get")


@app.command()
def export(
    features: str = "",
    snapshot_date: Optional[datetime] = None,
    output_file: str = "",
    metadata: str = "",
):
    typer.echo(f"Loading metadata...{metadata}")
    repo = RepoConfig.parse_yaml_file(metadata)
    fs = FeatureStore(repo)
    output = fs.export(features.split(","), snapshot_date, output_file)
    typer.echo(output)


@app.command()
def load(file: str, group: str = "", metadata: str = "", if_exists: str = "replace"):
    if_exists_list = ["replace", "append", "fail"]
    if if_exists not in if_exists_list:
        raise ValueError(f"if_exists must be one of {if_exists_list}")
    repo = RepoConfig.parse_yaml_file(metadata)
    pd.read_csv(file).to_sql(group, con=repo.engine, if_exists=if_exists)
    return None


if __name__ == "__main__":
    app()
