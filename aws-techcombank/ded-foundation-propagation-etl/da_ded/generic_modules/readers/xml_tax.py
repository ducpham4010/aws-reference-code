import xml.etree.ElementTree as ET
import xml.etree.cElementTree as ET

from marshmallow import Schema, fields
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from da_ded.utils import get_data_files_from_entry
from .base import BaseReader
from ..models.job_input import JobInput
from ...util.entry_file_utils import extract_s3_bucket, extract_s3_object_key
from ...util.s3_utils import read_s3_obj_body


class XMLTaxReaderConfigSchema(Schema):
    entry_file = fields.String(load_default=None)
    uri_list = fields.List(fields.String(), load_default=None)


class XMLTaxReaderConfig:
    def __init__(self, a_dict: dict):
        self.entry_file = a_dict["entry_file"]
        self.uri_list = a_dict["uri_list"]


def parse_reader_config(job_input: JobInput) -> XMLTaxReaderConfig:
    if job_input.reader_config:
        # new config version
        config_dict = job_input.reader_config
        config_dict = XMLTaxReaderConfigSchema().load(config_dict)
        config = XMLTaxReaderConfig(config_dict)
    else:
        # old legacy config fields that are used and are in JobInput
        config = XMLTaxReaderConfig(job_input.__dict__)
    return config


class XMLTaxReader(BaseReader):
    """Read XML Tax files directly from S3 bucket"""

    def read(self):
        config = parse_reader_config(self.job_input)
        return self._read_tax_xml(self.spark, config)

    @staticmethod
    def _parse_tax_xml(xml_string):
        dsNguoiNopThue = []

        parsedXML = ET.ElementTree(ET.fromstring(xml_string))

        ds_ND_DS_NNT = parsedXML.getroot().find('ND_DS_NNT')

        # 1. đây là get dữ liệu của ND_DS_CTIET
        ds_ND_DS_CTIET = ds_ND_DS_NNT.find('ND_DS_CTIET')
        for itemChiTiet in ds_ND_DS_CTIET:
            soThuTu = itemChiTiet.findtext('STT')
            maSoThue = itemChiTiet.findtext('MST')
            tenNguoiNopThue = itemChiTiet.findtext('TEN_NNT')
            ngaySinh = itemChiTiet.findtext('NGAY_SINH')
            loaiNNT = itemChiTiet.findtext('LOAI_NNT')
            trangThaiMST = itemChiTiet.findtext('TTHAI_MST')

            # 2. đây là get dữ liệu của TTIN_GIAYTO (sub select)
            # ds_TTIN_GIAYTO = ds_ND_DS_CTIET.find('ROW_CTIET/TTIN_GIAYTO')
            ds_TTIN_GIAYTO = itemChiTiet.find('TTIN_GIAYTO')

            # fix 5 loai giay to
            loaiGiayTo1 = ''
            tenLoaiGiayTo1 = ''
            soGiayTo1 = ''

            loaiGiayTo2 = ''
            tenLoaiGiayTo2 = ''
            soGiayTo2 = ''

            loaiGiayTo3 = ''
            tenLoaiGiayTo3 = ''
            soGiayTo3 = ''

            loaiGiayTo4 = ''
            tenLoaiGiayTo4 = ''
            soGiayTo4 = ''

            loaiGiayTo5 = ''
            tenLoaiGiayTo5 = ''
            soGiayTo5 = ''

            for itemGiayTo in ds_TTIN_GIAYTO:
                loaiGiayTo = itemGiayTo.findtext('LOAI_GIAYTO')
                tenLoaiGiayTo = itemGiayTo.findtext('TEN_LOAI_GIAYTO')
                soGiayTo = itemGiayTo.findtext('SO_GIAYTO')
                if loaiGiayTo == '1':
                    loaiGiayTo1 = loaiGiayTo
                    tenLoaiGiayTo1 = tenLoaiGiayTo
                    soGiayTo1 = soGiayTo
                elif loaiGiayTo == '2':
                    loaiGiayTo2 = loaiGiayTo
                    tenLoaiGiayTo2 = tenLoaiGiayTo
                    soGiayTo2 = soGiayTo
                elif loaiGiayTo == '3':
                    loaiGiayTo3 = loaiGiayTo
                    tenLoaiGiayTo3 = tenLoaiGiayTo
                    soGiayTo3 = soGiayTo
                elif loaiGiayTo == '4':
                    loaiGiayTo4 = loaiGiayTo
                    tenLoaiGiayTo4 = tenLoaiGiayTo
                    soGiayTo4 = soGiayTo
                elif loaiGiayTo == '5':
                    loaiGiayTo5 = loaiGiayTo
                    tenLoaiGiayTo5 = tenLoaiGiayTo
                    soGiayTo5 = soGiayTo
                else:
                    pass

            # row data
            rowChiTiet = (soThuTu, maSoThue, tenNguoiNopThue, ngaySinh, loaiNNT, trangThaiMST,
                          loaiGiayTo1, tenLoaiGiayTo1, soGiayTo1,
                          loaiGiayTo2, tenLoaiGiayTo2, soGiayTo2,
                          loaiGiayTo3, tenLoaiGiayTo3, soGiayTo3,
                          loaiGiayTo4, tenLoaiGiayTo4, soGiayTo4,
                          loaiGiayTo5, tenLoaiGiayTo5, soGiayTo5
                          )

            # table data
            dsNguoiNopThue.append(rowChiTiet)
        return dsNguoiNopThue

    @classmethod
    def _read_tax_xml(cls, spark: SparkSession, config: XMLTaxReaderConfig) -> DataFrame:
        if config.entry_file:
            data_files = get_data_files_from_entry(config.entry_file)
        else:
            data_files = config.uri_list

        # data_files_str = ','.join(data_files)
        # print(data_files_str)
        tax_schema = StructType([StructField("soThuTu", StringType(), True),
                                           StructField("maSoThue", StringType(), True),
                                           StructField("tenNguoiNopThue", StringType(), True),
                                           StructField("ngaySinh", StringType(), True),
                                           StructField("loaiNNT", StringType(), True),
                                           StructField("trangThaiMST", StringType(), True),
                                           StructField("loaiGiayTo1", StringType(), True),
                                           StructField("tenLoaiGiayTo1", StringType(), True),
                                           StructField("soGiayTo1", StringType(), True),
                                           StructField("loaiGiayTo2", StringType(), True),
                                           StructField("tenLoaiGiayTo2", StringType(), True),
                                           StructField("soGiayTo2", StringType(), True),
                                           StructField("loaiGiayTo3", StringType(), True),
                                           StructField("tenLoaiGiayTo3", StringType(), True),
                                           StructField("soGiayTo3", StringType(), True),
                                           StructField("loaiGiayTo4", StringType(), True),
                                           StructField("tenLoaiGiayTo4", StringType(), True),
                                           StructField("soGiayTo4", StringType(), True),
                                           StructField("loaiGiayTo5", StringType(), True),
                                           StructField("tenLoaiGiayTo5", StringType(), True),
                                           StructField("soGiayTo5", StringType(), True)])
        df = spark.createDataFrame([], schema=tax_schema)
        for file in data_files:
            bucket = extract_s3_bucket(file)
            key = extract_s3_object_key(file)
            xml_string = read_s3_obj_body(bucket, key)
            res = XMLTaxReader._parse_tax_xml(xml_string)
            _df = spark.createDataFrame(res, schema=tax_schema)
            df = df.union(_df)
            del res
            del _df

        yield df