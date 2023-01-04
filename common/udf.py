from datetime import datetime, date

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType, DateType


class TimeFunc:
    @staticmethod
    @fn.udf(returnType=StringType())
    def millisecond_to_datetime(timestamp):
        if not timestamp:
            return None
        
        timestamp = timestamp / 1000
        if timestamp > 4100688000:
            return "9999-12-31 00:00:00"
        
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    @fn.udf(returnType=DateType())
    def millisecond_to_date(timestamp):
        if not timestamp:
            return None
        
        timestamp = timestamp / 1000
        if timestamp > 4100688000:
            return date(year=9999, month=12, day=31)
        
        return datetime.fromtimestamp(timestamp).date()
    
    @staticmethod
    @fn.udf(returnType=DateType())
    def str_datetime_to_date(date_time):
        if not date_time:
            return None
        
        t = date_time.split(" ")
        return datetime.strptime(t[0], "%Y-%m-%d").date()

    @staticmethod
    @fn.udf(returnType=DateType())
    def datetime_to_date(t):
        if not t:
            return None
        return t.date()
    
    @staticmethod
    @fn.udf(returnType=DateType())
    def time_to_date(t):
        if not t:
            return None
        if isinstance(t, str):
            t = t.split(" ")
            return datetime.strptime(t[0], "%Y-%m-%d").date()
        if isinstance(t, datetime):
            return t.date()
        
        if isinstance(t, int):
            return datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    @fn.udf(returnType=StringType())
    def timestamp_to_datetime(timestamp):
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    @fn.udf(returnType=DateType())
    def timestamp_to_date(timestamp):
        return datetime.fromtimestamp(timestamp).date()
