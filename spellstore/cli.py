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
def load(input_file: str, group: str = "", metadata: str = "", if_exists: str = "replace"):
    if_exists_list = ["replace", "append", "fail"]
    if if_exists not in if_exists_list:
        raise ValueError(f"if_exists must be one of {if_exists_list}")
    repo = RepoConfig.parse_yaml_file(metadata)
    pd.read_csv(input_file).to_sql(group, con=repo.engine, if_exists=if_exists)
    return None


@app.command()
def join(
    input_file: str,
    entity_column: str = "",
    event_timestamp_column: Optional[str] = "",
    features: str = "",
    metadata: str = "",
):
    if entity_column == "":
        raise ValueError("Entity column must be provided")
    if event_timestamp_column == "":
        event_timestamp_column = None
    repo = RepoConfig.parse_yaml_file(metadata)
    entity_df = pd.read_csv(input_file)
    feature_list = features.split(",")
    fs = FeatureStore(repo_config=repo)
    output = fs.join(entity_df, entity_column, event_timestamp_column, feature_list)
    typer.echo(output.to_markdown(index=False))


if __name__ == "__main__":
    app()
