from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as fn

from .saver import Saver


class ETL(Saver):
    def __init__(self, data_source: dict, output_uri: str, partition_keys: list = None, app_name: str = "demo"):
        super(ETL, self).__init__(output_uri, partition_keys)
        self.ctx = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
        self.data_source = data_source
        self.save_mode = "append"

    def prerequisite(self):
        pass

    def read(self) -> dict:
        result = dict()
        for key in self.data_source:
            item = self.data_source[key]
            if item.endswith("csv"):
                source_df = self.ctx.read.option("header", "true").csv(item)
            elif item.endswith("json"):
                source_df = self.ctx.read.option("multiLine", "true").json(item)
            elif item.endswith("parquet"):
                source_df = self.ctx.read.parquet(item)
            else:
                raise ValueError("data source not support")

            result[key] = source_df
        return result

    def run(self):
        """
        不应该被重载的方法
        :return:
        """
        self.prerequisite()
        df_dict = self.read()
        trans_dict = dict()
        for data_key in df_dict:
            extract_sql = self.extract(data_key)
            df = df_dict[data_key]
            if df and extract_sql:
                df.createOrReplaceTempView(data_key)
                # Create a DataFrame of the top 10 restaurants with the most Red violations
                df = self.ctx.sql(extract_sql)

            if not df:
                raise ValueError(f"no data for data {data_key}")

            df = self.transform(data_key, df)
            trans_dict[data_key] = df

        if trans_dict:
            c_df = self.combine(trans_dict)

            c_df = self.before_save(c_df)

            if not c_df:
                raise ValueError("method before_save should return  DataFrame")

            self.save(c_df, self.save_mode)
            self.after_save(c_df)

    def extract(self, data_key: str) -> str:
        raise NotImplementedError("need implement in subclass")

    def transform(self, data_key: str, df: DataFrame) -> DataFrame:
        raise NotImplementedError("need implement in subclass")

    def combine(self, df_dict: dict) -> DataFrame:
        raise NotImplementedError("need implement in subclass")

    def before_save(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("need implement in subclass")

    def after_save(self, df: DataFrame):
        raise NotImplementedError("need implement in subclass")

    def merge(self, df1: DataFrame, df2: DataFrame, dims: list, method: str = "outer"):
        """
        merge : combine multiple measurements to one DataFrame
        :param df1:
        :param df2:
        :param dims: 需要拼接的维度
        :param method: 拼接的方法
        :return:
        """

        # rename all columns avoid the duplicate column after join
        measurement_columns = [col for col in df2.columns if col not in dims]
        dim_columns = [fn.col(key).alias("{0}1".format(key)) for key in dims]

        df2 = df2.select(*dim_columns, *measurement_columns)
        df2.show(1)
        # join condition
        cond = [df1[key] == df2["{0}1".format(key)] for key in dims]

        df = df1.join(df2, cond, method)
        df.show(1)
        for key in dims:
            new_key = "{0}1".format(key)

            if method in ["outer", "right"]:
                df = df.withColumn(key, fn.when(fn.col(key).isNull(), fn.col(new_key)).otherwise(fn.col(key)))

            df = df.drop(new_key)

        df = df.cache()
        return df
