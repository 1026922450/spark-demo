from pyspark.sql import DataFrame


class Saver:
    def __init__(self, output_uri: str, partition_keys: list = None):
        self.output_uri = output_uri
        self.partition_keys = partition_keys

    def save(self, df: DataFrame, save_mode: str = "append"):
        """
        :param df: data frame needed to be save
        :param save_mode: "overwrite" or "append" , default value is "append"
        :return:
        """
        if self.output_uri.endswith(".csv"):
            df.write.option("header", "true").mode(save_mode).csv(self.output_uri)
            return

        if self.output_uri.endswith(".json"):
            df.write.mode(save_mode).json(self.output_uri)
            return

        if self.output_uri.endswith(".parquet"):
            fmt = "parquet"
            if self.partition_keys:
                df.write.format(fmt).mode(save_mode).saveAsTable(self.output_uri, partitionBy=self.partition_keys)
            else:
                df.write.format(fmt).mode(save_mode).saveAsTable(self.output_uri)
            return

        raise NotImplementedError("NOT Support output format")





