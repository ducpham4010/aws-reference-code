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

def load_movies_from_json(table, jsondata, dynamodb=None):
    for movie in jsondata:
        year = int(movie['year'])
        title = movie['title']
        print("Adding movie:", year, title)
        table.put_item(Item = movie)

def add_movie(table, title, year, plot, rating):
    try:
        table.put_item(
            Item={
                'year': year,
                'title': title,
                'info': {'plot': plot, 'rating': Decimal(str(rating))}})
    except ClientError as err:
        logger.error(
            "Couldn't add movie %s to table %s. Here's why: %s: %s",
            title, table.name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

def get_movie(table, title, year):
    try:
        response = table.get_item(Key = {'year': year, 'title': title})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response["Item"]

def update_movie(table, title, year, rating, plot):
        """
        Updates rating and plot data for a movie in the table.

        :param title: The title of the movie to update.
        :param year: The release year of the movie to update.
        :param rating: The updated rating to the give the movie.
        :param plot: The updated plot summary to give the movie.
        :return: The fields that were updated, with their new values.
        """
        try:
            response = table.update_item(
                Key={'year': year, 'title': title},
                UpdateExpression="set info.rating=:r, info.plot=:p",
                ExpressionAttributeValues={
                    ':r': Decimal(str(rating)), ':p': plot},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update movie %s in table %s. Here's why: %s: %s",
                title, table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']

def delete_underrated_movie(table, title, year, rating):
        """
        Deletes a movie only if it is rated below a specified value. By using a
        condition expression in a delete operation, you can specify that an item is
        deleted only when it meets certain criteria.

        :param title: The title of the movie to delete.
        :param year: The release year of the movie to delete.
        :param rating: The rating threshold to check before deleting the movie.
        """
        try:
            table.delete_item(
                Key={'year': year, 'title': title},
                ConditionExpression="info.rating <= :val",
                ExpressionAttributeValues={":val": Decimal(str(rating))})
        except ClientError as err:
            if err.response['Error']['Code'] == "ConditionalCheckFailedException":
                logger.warning(
                    "Didn't delete %s because its rating is greater than %s.",
                    title, rating)
            else:
                logger.error(
                    "Couldn't delete movie %s. Here's why: %s: %s", title,
                    err.response['Error']['Code'], err.response['Error']['Message'])
            raise

def delete_movie_table(table):
    table.delete()



def add_log(table, partition_key, soft_key, status):
    try:
        table.put_item(
            Item={
                'jobname': partition_key,
                'ppn_dt': soft_key,
                'status': status })
    except ClientError as err:
        logger.error(
            "Couldn't add movie %s to table %s. Here's why: %s: %s",
            partition_key, table.name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('table_log')
    table.load()

## --Insert data from json file
    # with open("D:\Tailieu\\fss\python\\aws\dynamodb\moviedata.json") as json_file:
    #     movie_list = json.load(json_file, parse_float = Decimal)
    # load_movies_from_json(table, movie_list)

## --Insert data
    # add_movie(table, "BlackAdma", 2022, "A New SuperHero In DCEU", 8.8)
    add_log(table, "Job", 20221116, "1")

## --Get data
    # movie = get_movie(table, "BlackAdma", 2022)
    # if movie:  
    #     print(movie)

## --Update data
    # update_movie(table, "BlackAdma", 2022, 9.0, 'A new Anti-hero in DCEU')

## --Delete item
    # delete_underrated_movie(table, "BlackAdma", 2022, 9.0)

## -Detete item
    # delete_movie_table(table)

