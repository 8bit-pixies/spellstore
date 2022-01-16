"""
Usage:

```console
python -m spellbook.cli get meta entity --config config.yml

```

"""

import typer

from spellbook.base import RepoConfig

app = typer.Typer()
meta_app = typer.Typer()
app.add_typer(meta_app, name="meta")


@meta_app.command()
def all(metadata: RepoConfig):
    metadata = RepoConfig.parse_yaml_file(metadata)
    typer.echo(metadata.print_meta())


@meta_app.command()
def entity(metadata: RepoConfig):
    metadata = RepoConfig.parse_yaml_file(metadata)
    typer.echo(metadata.print_entity())


@meta_app.command()
def feature(metadata: RepoConfig):
    metadata = RepoConfig.parse_yaml_file(metadata)
    typer.echo(metadata.print_feature())


@meta_app.command()
def group(metadata: RepoConfig):
    metadata = RepoConfig.parse_yaml_file(metadata)
    typer.echo(metadata.print_group())
