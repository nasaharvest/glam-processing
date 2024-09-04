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


@cli.command()
@click.argument("dataset-id", type=str)
def info(dataset_id):
    """Get info on supported products"""
    from .download import Downloader, SUPPORTED_DATASETS, EARTHDATA_DATASETS

    if dataset_id in SUPPORTED_DATASETS:
        if dataset_id in EARTHDATA_DATASETS:
            downloader = Downloader(dataset_id)
            click.echo(downloader.info())
        else:
            click.echo(f"Summary information for {dataset_id} not available")
    else:
        click.echo(f"Dataset {dataset_id} not found in list of supported datasets")


if __name__ == "__main__":
    cli()
