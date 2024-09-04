import sys

import click


@click.group()
def cli():
    """Glam Processing Tools"""
    pass


@cli.command()
@click.option("-s", "--strategy", default="interactive", type=str)
@click.option("-p", "--persist", default=True, type=bool)
def auth(strategy, persist):
    """Authenticate earthaccess with NASA Earthdata credentials"""
    from .earthdata import authenticate

    authenticated = authenticate()

    click.echo(
        "Successfully authenticated!" if authenticated else "Failed to authenticate"
    )


@cli.command()
def list():
    """List supported products"""
    from .download import SUPPORTED_DATASETS

    click.echo(f"Supported product datasets: {SUPPORTED_DATASETS}")


if __name__ == "__main__":
    cli()
