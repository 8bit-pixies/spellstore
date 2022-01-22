"""
Usage:

```console
python -m spellstore.cli get meta entity --config config.yml

```

"""

import typer

from spellstore.base import RepoConfig

app = typer.Typer()
meta_app = typer.Typer()
app.add_typer(meta_app, name="meta")


@meta_app.command()
def all(metadata: str = ""):
    repo = RepoConfig.parse_yaml_file(metadata)
    typer.echo(repo.print_meta())


@meta_app.command()
def entity(metadata: str = ""):
    repo = RepoConfig.parse_yaml_file(metadata)
    typer.echo(repo.print_entity())


@meta_app.command()
def feature(metadata: str = ""):
    repo = RepoConfig.parse_yaml_file(metadata)
    typer.echo(repo.print_feature())


@meta_app.command()
def group(metadata: str = ""):
    repo = RepoConfig.parse_yaml_file(metadata)
    typer.echo(repo.print_group())
