"""
Usage:

```console
python -m spellbook.cli get meta entity --config config.yml

```

"""

from typing import Optional

import typer

from spellbook.metadata import MetaData

app = typer.Typer()
meta_app = typer.Typer()
app.add_typer(meta_app, name="meta")


@meta_app.command()
def all(metadata: str = ""):
    metadata = MetaData.parse_yaml_file(metadata)
    typer.echo(metadata.print_meta())


@meta_app.command()
def entity(metadata: str = ""):
    metadata = MetaData.parse_yaml_file(metadata)
    typer.echo(metadata.print_entity())


@meta_app.command()
def feature(metadata: str = ""):
    metadata = MetaData.parse_yaml_file(metadata)
    typer.echo(metadata.print_feature())


@meta_app.command()
def group(metadata: str = ""):
    metadata = MetaData.parse_yaml_file(metadata)
    typer.echo(metadata.print_group())
