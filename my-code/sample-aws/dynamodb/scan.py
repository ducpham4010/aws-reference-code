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


def scan_movies(table, year_range):
        """
        Scans for movies that were released in a range of years.
        Uses a projection expression to return a subset of data for each movie.

        :param year_range: The range of years to retrieve.
        :return: The list of movies released in the specified years.
        """
        movies = []
        scan_kwargs = {
            'FilterExpression': Key('year').between(year_range['first'], year_range['second']),
            'ProjectionExpression': "#yr, title, info.rating",
            'ExpressionAttributeNames': {"#yr": "year"}}
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs['ExclusiveStartKey'] = start_key
                response = table.scan(**scan_kwargs)
                movies.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None # scan moi lan chi tra ve 1 MB nen can kiem tra xem da tra ve du lieu chua
        except ClientError as err:
            logger.error(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        return movies



if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Movies')
    table.load()

    year_range = {'first' : 1999, 'second' : 2012}
    data = scan_movies(table, year_range)
    print(data)

