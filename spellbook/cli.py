import typer

from spellbook import cli_get

app = typer.Typer()
app.add_typer(cli_get.app, name="get")


if __name__ == "__main__":
    app()
