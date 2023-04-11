import boto3
from botocore.exceptions import ClientError
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)
def my_logging_handler(event, context):
    logger.info('got event{}'.format(event))
    logger.error('something went wrong')

dynamodb_resource = boto3.resource('dynamodb')
class Dynamodb:
    """Encapsulates an Amazon DynamoDB table of movie data."""
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def create_table(self, table_name, partition_key, soft_key):
        """
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the release year of the movie as the partition key and the
        title as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': partition_key, 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': soft_key, 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': partition_key, 'AttributeType': 'S'},
                    {'AttributeName': soft_key, 'AttributeType': 'N'}
                ],
                BillingMode = 'PAY_PER_REQUEST'
                #ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table


if __name__ == '__main__':
    movie_table = Dynamodb(dynamodb_resource)
    movie_table.create_table("table_log", "jobname", "ppn_dt")
    print("Done")

