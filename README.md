# Kafka

## Установка

Клон репозитория
```command
git clone git@github.com:YaraKoba/My_kafka.git
```

Перейти в деректорию `my_kafka\docker_with_sasl`

```command
docker-compose -f docker-compose-kafka-sasl.yml up -d
```

- Установить соеденение в Offset
- Создать топик `from_click`

Для питона
```command
pip install -r requirements.txt
```

Создать файл с ключами в директории с скриптом и указать к ниму путь в `connect_db`
```json
{
    "server": [
      {
        "host": "your_click_host",
        "user": "your_user",
        "password": "your_pass",
        "port": 9000
      }
    ]
  }
```

  ## Запуск

  ```commad
  python ./scripts/inser_to_kafka.py
  ```
  ```commad
  python ./scripts/read_from_kafka.py
  ```


## Проверка

![image](https://github.com/YaraKoba/My_kafka/assets/102355943/072353bb-5d44-4ab2-996a-b1b567ef2900)

