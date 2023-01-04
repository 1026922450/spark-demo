
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as fn

from .saver import Saver


class ETL(Saver):
    def __init__(self, data_source: str, output_uri: str, partition_keys: list = None, app_name: str = "demo"):
        super(ETL, self).__init__(output_uri, partition_keys)
        self.ctx = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
        self.data_source = data_source
        self.save_mode = "append"

    def prerequisite(self):
        pass

    def read(self) -> DataFrame:
        if self.data_source.endswith("csv"):
            source_df = self.ctx.read.option("header", "true").csv(self.data_source)
        elif self.data_source.endswith("json"):
            source_df = self.ctx.read.option("multiLine", "true").json(self.data_source)
        elif self.data_source.endswith("parquet"):
            source_df = self.ctx.read.parquet(self.data_source)
        else:
            raise ValueError("data source not support")
        return source_df

    def run(self):
        """
        不应该被重载的方法
        :return:
        """
        self.prerequisite()
        df = self.read()
        temp_tb_view = "temp_data_tb"
        extract_sql = self.extract(temp_tb_view)
        if df and extract_sql:
            df.createOrReplaceTempView(temp_tb_view)
            # Create a DataFrame of the top 10 restaurants with the most Red violations
            df = self.ctx.sql(extract_sql)

        if not df:
            print("No data for this task")
            return
        df = self.transform(df)
        if not df:
            raise ValueError("method transform should return  DataFrame")
        df = self.before_save(df)
        if not df:
            raise ValueError("method before_save should return  DataFrame")
        self.save(df, self.save_mode)
        self.after_save(df)

    def extract(self, temp_view_name: str) -> str:
        raise NotImplementedError("need implement in subclass")

    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("need implement in subclass")

    def before_save(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("need implement in subclass")

    def after_save(self, df: DataFrame):
        raise NotImplementedError("need implement in subclass")


