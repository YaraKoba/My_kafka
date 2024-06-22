from clickhouse_driver import Client
import pandas as pd
import json

def openKeyfile():

    with open(r"C:\Users\Ярослав\Documents\MY-projects\Kafka\my_kafka\scripts\keys\wb_key_ch.json") as json_file:
        param_сonnect = json.load(json_file)

    return param_сonnect



class ClickHouseHook():
    def __init__(self):
        pass

    def get_conn(self) -> Client:
        param_connect = openKeyfile()
        return Client(param_connect['server'][0]['host'],
                            user=param_connect['server'][0]['user'],
                            password=param_connect['server'][0]['password'],
                            port=param_connect['server'][0]['port'],
                            verify=False,
                            database='default',
                            settings={"numpy_columns": True, 'use_numpy': True},
                            compression=True)

    def execute(self, sql: str):
        conn = self.get_conn()
        conn.execute(sql)

    def query_dataframe(self, sql: str) -> pd.DataFrame:
        conn = self.get_conn()
        result = conn.query_dataframe(sql)

        return result

