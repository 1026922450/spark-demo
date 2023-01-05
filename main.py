import argparse
from common.etl import ETL
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, BooleanType
from datetime import date


class DemoETL(ETL):

    def extract(self, data_key: str) -> str:
        if data_key == "trade":
            today = date.today()
            return f"""
              SELECT *, {today} as etl_date
              FROM {data_key}
              """

    def transform(self, data_key: str, df: DataFrame) -> DataFrame:
        print(f"transform {data_key}")
        if data_key == "trade":
            df.printSchema()
            df = df.withColumn("const_column", fn.lit(100))
            df = df.withColumn("IRATE_USD_SWP_UL3_01D", df['IRATE_USD_SWP_UL3_01D'] + df['IRATE_USD_SWP_UL3_07D'])
            df = df.withColumn("udf_column",
                               DemoETL._compare_value(df['IRATE_USD_SWP_UL3_01M'], df['IRATE_USD_SWP_UL3_07D']))

        return df

    def combine(self, df_dict: dict) -> DataFrame:
        print("begin combine")

        df_trade = self.df_form('trade', df_dict)

        df_market2 = df_dict['market2']
        df_market3 = df_dict['market3']

        df = self.merge(df_trade, df_market2, ["date"])
        df = self.merge(df, df_market3, ["t_check"])

        # other transform
        df = df.withColumn("combine_done", df['IRATE_USD_SWP_UL3_01D'] * 100)

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
    source_folder = "/Users/fugui/Work/project/bimaw/data/"
    data_source = {
        "trade": f"{source_folder}/trade.csv",
        "market2": f"{source_folder}/market2.csv",
        "market3": f"{source_folder}/market3.csv"
    }
    output_uri = "/Users/fugui/Work/project/bimaw/output/test.csv"
    app(data_source, output_uri)
