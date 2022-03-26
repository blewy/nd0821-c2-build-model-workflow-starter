#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import os

import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()

    df = pd.read_csv(artifact_path)

    # Drop the Outliers
    logger.info("Dropping Outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    logger.info("Convert to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    filename = args.output_artifact
    df.to_csv(filename, index=False)

    # OTE: Remember to use index=False when saving to CSV,
    # otherwise the data checks in the next step might fail
    # because there will be an extra index column
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)

    logger.info("Logging artifact")
    run.log_artifact(artifact)

    os.remove(filename)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Fully-qualified name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of data output",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Full description of the data prepared on this step",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Min value accepted for the price of the houses",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Max value accepted for the price of the houses",
        required=True
    )

    args = parser.parse_args()

    go(args)
