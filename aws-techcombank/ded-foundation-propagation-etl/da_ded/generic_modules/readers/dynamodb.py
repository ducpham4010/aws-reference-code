from .base import BaseReader
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from da_ded.logger import logger
import pandas as pd
from pyspark.sql.functions import to_json, struct
import json
from pyspark.sql.functions import current_timestamp, current_date, lit  # import functions for use


def nested_fields(df, except_list, path_type, std_index, end_index):
    tmp_fin = pd.DataFrame()
    for element in df.select((struct([df[x] for x in df.columns]))).collect():
        for i in element:
            tmp_dict = dict()
            tmp_dict[path_type] = [dict({"system":"Amazon Web Services (AWS)"}),dict({"database":"AwsDataCatalog"})]
            tmp_dict[path_type].extend([{k: v} for (k, v) in i.asDict().items()][std_index:(end_index + 1)])
            for e in except_list:
                tmp_dict[e] = i.asDict()[e]
            add_df = pd.DataFrame(tmp_dict)
            tmp_fin = tmp_fin.append(add_df)
    series_tmp = pd.DataFrame(tmp_fin.groupby(except_list)[path_type].apply(list))
    return series_tmp

# Filter DF
# "
# Filter Pyspark DynamicFrame and return as a Pyspark DataFrame
# + df_list: dictionary of unnested dynamic frame using relationalize()
# + option: name of unnested object want to get
# + filter_list: array of columns want to get
# "


def filter_df(df_list, option, filter_list):
    tmp_df = df_list[option].select_fields(filter_list).toDF()
    return tmp_df


def df_rename(df, new_names_list):
    try:
        df = df.toDF(*new_names_list)
    except:
        print("Something went wrong")

    return df


class DynamoDBReader(BaseReader):
    def __init__(self, job_input, spark: SparkSession, glue_context):
        super().__init__(job_input, spark, glue_context)
        dynamo_datalineage = self.job_input.dynamo_datalineage
        self.table_name = dynamo_datalineage["table_name"]
        self.tmp_path = dynamo_datalineage["tmp_path"]
        self.tree_lineage_path = dynamo_datalineage["tree_lineage_path"]
        self.json_lineage_path = dynamo_datalineage["json_lineage_path"]
        self.lineage_write_mode = dynamo_datalineage["lineage_write_mode"]
        self.lineage_file_format = dynamo_datalineage["lineage_file_format"]

    def read(self):
        lineage, s3_current = self._get_current_lineage()
        self._write_json_lineage(lineage)
        return s3_current

    def _get_current_lineage(self):

        # Read snapshot data from DynamoDB table
        execution = self.glue_context.create_dynamic_frame.from_options("dynamodb",
                                                                        connection_options={
                                                                            "dynamodb.input.tableName": self.table_name,
                                                                        }
                                                                        )

        # Un-nested snapshot records and save each object onto a dictionary for addressing later
        unnested = execution.relationalize("root", self.tmp_path)
        keys_list = unnested.keys()
        DF_list = dict()
        for i in keys_list:
            DF_list[i] = unnested.select(i)

        # Processing un-nested "inputs" array in each "logic"
        # of "logics" object (from DynamoDB)
        filter_list = ["id",
                       "`logics.val.inputs.val.columns`",
                       "`logics.val.inputs.val.database`",
                       "`logics.val.inputs.val.table`",
                       "`logics.val.inputs.val.path`",
                       "`logics.val.inputs.val.source`"
                       ]
        input_df = filter_df(DF_list, 'root_logics.val.inputs', filter_list)

        # rename for better query
        new_columns = ['output_id', 'column_id',
                       'schema', 'table',
                       'path', 'source'
                       ]
        input_df = df_rename(input_df, new_columns)

        # Processing un-nested "columns" array in each "input"
        # of "inputs" object (from DynamoDB)
        filter_list = ["id", "index", "`logics.val.inputs.val.columns.val`"]
        input_columns = filter_df(DF_list, "root_logics.val.inputs.val.columns", filter_list)

        new_columns = ['column_id', 'index', 'column']
        input_columns = df_rename(input_columns, new_columns)

        input_df_col_lv = input_columns.join(input_df, 'column_id', 'left')
        input_df_col_lv = input_df_col_lv.select("column_id", "output_id", "index",
                                                 "schema", "table", "column", "path", "source")

        # Create "input" dataframe
        # with fully unested with input column information
        # and indexing with "output_id","column_id", "index"

        except_list = ['output_id', 'column_id', 'index']
        type_path = 'src_path'

        series_input = nested_fields(input_df_col_lv, except_list, type_path, 3, 5)

        # Processing un-nested "output" in each "logic"
        # of "logics" object (from DynamoDB)
        filter_list = ["id",
                       "`logics.val.inputs`",
                       "`logics.val.output.database`",
                       "`logics.val.output.table`",
                       "`logics.val.output.column`",
                       "`logics.val.output.path`",
                       "`logics.val.output.source`"
                       ]
        output_df = filter_df(DF_list, "root_logics", filter_list)

        new_columns = ['job_id', 'output_id', 'schema',
                       'table', 'column',
                       'path', 'source'
                       ]

        output_df = df_rename(output_df, new_columns)

        # Create "output" dataframe
        # with fully unested with output column information
        # and indexing with "job_id","output_id"
        except_list = ['job_id', 'output_id']
        type_path = 'trg_path'
        series_output = nested_fields(output_df, except_list, type_path, 2, 4)

        # Join output and input dataframe to produce final dataframe
        fin = series_output.join(series_input).reset_index()

        # Produce lineage under json format
        series_output_Spark = self.spark.createDataFrame(fin.reset_index())

        # Adding "mapping" and "source_code" object from "root" object (relationalized before)
        root_df = DF_list["root"].toDF()

        output_json = series_output_Spark.join(root_df, series_output_Spark.job_id == root_df.logics, "outer").select(
            'job', 'src_path', 'trg_path'                                                                                                                        , 'description')
        new_names_list = ['mapping', 'src_path', 'trg_path', 'source_code']
        output_json = df_rename(output_json, new_names_list)

        # Produce lineage snapshot for historical record on S3
        s3_current = ((root_df.join(output_df, output_df.job_id == root_df.logics, "outer"))
                      .join(input_df_col_lv, on='output_id'))\
            .select('job',
                    input_df_col_lv['table'],
                    input_df_col_lv['schema'],
                    input_df_col_lv['path'],
                    input_df_col_lv['source'],
                    output_df['table'],
                    output_df['schema'],
                    output_df['path'],
                    output_df['source'],
                    root_df['description']
                    )
        s3_current = s3_current.select(
            ["*", lit(current_date()).alias("ds_partition_date"), lit("1").alias("is_current")])
        # Rename
        new_columns = ['job_name'
                       , 'input_table', 'input_schema', 'input_path', 'input_source'
                       , 'output_table', 'output_schema', 'output_path', 'output_source'
                       , 'description', 'ds_partition_date', "is_current"
                       ]
        s3_current = df_rename(s3_current, new_columns)

        return json.loads(output_json.toPandas().to_json(orient='records')), s3_current         

    def _write_json_lineage(self, lineage):
        tree = self._read_tree_lineage()
        data = [("1.0", tree.collect(), self.spark.createDataFrame(lineage).collect())]
        lineage_df = self.spark.sparkContext.parallelize(data).toDF()
        new_columns = ["version", "tree", "lineages"]
        lineage_df = df_rename(lineage_df, new_columns)
        lineage_df.repartition(1).write.mode(self.lineage_write_mode).format(
            self.lineage_file_format).save(self.json_lineage_path)

    def _read_tree_lineage(self):
        return self.spark.read.json(self.tree_lineage_path)