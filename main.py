import argparse
from common.etl import ETL
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, BooleanType
from datetime import date


class DemoETL(ETL):

    def extract(self, temp_view_name) -> str:
        today = date.today()
        return f"""
        SELECT *, {today} as etl_date
        FROM {temp_view_name}
        """

    def transform(self, df: DataFrame) -> DataFrame:
        df.printSchema()
        df = df.withColumn("const_column", fn.lit(100))
        df = df.withColumn("sum_column", df['IRATE_USD_SWP_UL3_01D'] + df['IRATE_USD_SWP_UL3_07D'])
        df = df.withColumn("udf_column", DemoETL._compare_value(df['IRATE_USD_SWP_UL3_01D'], df['IRATE_USD_SWP_UL3_07D']))
        return df

    def before_save(self, df: DataFrame) -> DataFrame:
        return df

    def after_save(self, df: DataFrame):
        return df

    @staticmethod
    @fn.udf(returnType=BooleanType())
    def _compare_value(column1, column2):
        return column1 > column2


def app(data_source, output_uri):
    etl = DemoETL(data_source, output_uri)
    etl.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    print(args.data_source)
    print(args.output_uri)
    app(args.data_source, args.output_uri)
