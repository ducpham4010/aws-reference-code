from decimal import Decimal
from io import BytesIO
import json
import logging
import os
from pprint import pprint
import requests
from zipfile import ZipFile
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
def my_logging_handler(event, context):
    logger.info('got event{}'.format(event))
    logger.error('something went wrong')


# Query items by using a key condition expression
def query_movies(table, year):
        """
        Queries for movies that were released in the specified year.

        :param year: The year to query.
        :return: The list of movies that were released in the specified year.
        """
        try:
            response = table.query(KeyConditionExpression=Key('year').eq(year))
        except ClientError as err:
            logger.error(
                "Couldn't query for movies released in %s. Here's why: %s: %s", year,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

# Query items and project them to return a subset of data
def query_and_project_movies(table, year, title_bounds):
        """
        Query for movies that were released in a specified year and that have titles
        that start within a range of letters. A projection expression is used
        to return a subset of data for each movie.

        :param year: The release year to query.
        :param title_bounds: The range of starting letters to query.
        :return: The list of movies.
        """
        try:
            response = table.query(
                ProjectionExpression="#yr, title, info.genres, info.actors[0]",
                ExpressionAttributeNames={"#yr": "year"},
                KeyConditionExpression=(
                    Key('year').eq(year) &
                    Key('title').between(title_bounds['first'], title_bounds['second'])))
        except ClientError as err:
            if err.response['Error']['Code'] == "ValidationException":
                logger.warning(
                    "There's a validation error. Here's the message: %s: %s",
                    err.response['Error']['Code'], err.response['Error']['Message'])
            else:
                logger.error(
                    "Couldn't query for movies. Here's why: %s: %s",
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            return response['Items']


if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Movies')
    table.load()

    data = query_movies(table, 2013)
    print(data)
