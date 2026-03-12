import sys
import os

import click


@click.group()
def cli():
    """Glam Processing Tools"""
    pass


@cli.command()
@click.option("-s", "--strategy", default="interactive", type=str)
@click.option("-p", "--persist", default=True, type=bool)
def auth(strategy, persist):
    """Authenticate with NASA Earthdata and CDSE S3 credentials"""
    from .earthdata import authenticate

    # Authenticate with NASA Earthdata
    authenticated = authenticate()
    click.echo(
        "Successfully authenticated with NASA Earthdata!" if authenticated else "Failed to authenticate with NASA Earthdata"
    )

    # Check and prompt for CDSE S3 credentials
    click.echo("\n--- CDSE S3 Credentials ---")
    
    cdse_access_key = os.environ.get("CDSE_S3_ACCESS_KEY")
    cdse_secret_key = os.environ.get("CDSE_S3_SECRET_KEY")
    
    if cdse_access_key and cdse_secret_key:
        click.echo("CDSE S3 credentials already set in environment.")
        update_cdse = click.confirm("Would you like to update them?", default=False)
        if not update_cdse:
            return
    
    # Prompt for CDSE credentials
    click.echo("Please provide your CDSE S3 credentials.")
    click.echo("(Get these from https://dataspace.copernicus.eu/)")
    
    cdse_access_key = click.prompt("CDSE S3 Access Key", hide_input=False)
    cdse_secret_key = click.prompt("CDSE S3 Secret Key", hide_input=True)
    
    # Set environment variables
    os.environ["CDSE_S3_ACCESS_KEY"] = cdse_access_key
    os.environ["CDSE_S3_SECRET_KEY"] = cdse_secret_key
    
    click.echo("CDSE S3 credentials set successfully!")
    
    if persist:
        # Optionally save to a config file for persistence across sessions
        click.echo(
            "\nTo persist these credentials across sessions, add the following to your shell profile:"
        )
        click.echo(f'export CDSE_S3_ACCESS_KEY="{cdse_access_key}"')
        click.echo(f'export CDSE_S3_SECRET_KEY="{cdse_secret_key}"')


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
