import click
import logging


@click.command()
def cli(x):
    main()


@click.option("--name")
def main(name):
    # Execute custom task
    logging.info(f"Hello {name}")

    # save output
    open("_SUCCESS", "w").close()


if __name__ == "__main__":
    cli()
