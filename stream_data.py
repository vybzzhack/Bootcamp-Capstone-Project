
import requests
import json
from confluent_kafka import Producer

# Alpha Vantage API Configuration
api_key = ''
symbol = 'GOOGL'  # Replace with the desired stock symbol
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}'

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': '',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '',
    'sasl.password': '',
}

producer = Producer(**conf)
topic = 'stock_data'

def fetch_and_publish_data():
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        if 'Time Series (1min)' in data:
            realtime_data = data['Time Series (1min)']
            for timestamp, quote in realtime_data.items():
                data_point = {
                    'symbol': symbol,
                    'timestamp': timestamp,
                    'open': quote['1. open'],
                    'high': quote['2. high'],
                    'low': quote['3. low'],
                    'close': quote['4. close'],
                    'volume': quote['5. volume']
                }

                # Convert data to JSON and produce to Kafka
                producer.produce(topic, json.dumps(data_point).encode('utf-8'))
                producer.flush()
                print(f"Produced data to Kafka: {data_point}")

    else:
        print(f"Error: {response.status_code}")

if __name__ == "__main__":
    while True:
        fetch_and_publish_data()

