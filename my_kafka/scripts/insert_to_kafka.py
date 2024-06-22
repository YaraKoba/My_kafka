from confluent_kafka import Producer
from connect_db import ClickHouseHook
import json

config = {
    'bootstrap.servers': 'localhost:9093',  # адрес Kafka сервера
    'client.id': 'simple-producer',
    'sasl.mechanism':'PLAIN',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.username': 'admin',
    'sasl.password': 'admin-secret'
}

def main():
    producer = Producer(**config)
    client = ClickHouseHook()

    # Получаю массив словарей из клика
    data_from_click = get_data(client)

    # Пробегаюсь по массиву и отправляю строки в кафку
    for row in data_from_click:
        row_json = json.dumps(row)
        send_message_kafka(data=row_json, topic_name='from_click', producer=producer)

    producer.flush()



def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def send_message_kafka(data: str, topic_name: str, producer: Producer):
    try:
        # Асинхронная отправка сообщения
        producer.produce(topic_name, data.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Пуллинг для обработки обратных вызовов
    except BufferError:
        print(f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")


def get_data(client: ClickHouseHook) -> list[dict]:
    sql_query = """
        select gi_id, qty_shk_inc, plan_qty
        from datamart.goods_income_big final
        where dt_load >= today()
            and qty_shk_inc > 0
        limit 100
    """

    df = client.query_dataframe(sql_query)

    return df.to_dict('records')


if __name__ == '__main__':
    main()
