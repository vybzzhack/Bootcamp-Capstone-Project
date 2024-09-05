
import requests
from pymongo import MongoClient



# Replace 'YOUR_API_KEY' with your actual Alpha Vantage API key
api_key = ''

# Define the endpoint and parameters for the API call
url = 'https://www.alphavantage.co/query'
params = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'GOOGL',  # Replace 'AAPL' with the desired stock symbol
    'interval': '1min',  # Specify the desired time interval
    'apikey': api_key
}

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Replace with your MongoDB URI if different
db = client['Stockdata']  # Database name
collection = db['real_time_stock']  # Collection name

# Make the API call
response = requests.get(url, params=params)

# Check if the API call was successful
if response.status_code == 200:
    data = response.json()

    # Extract the real-time stock data from the response
    realtime_data = data['Time Series (1min)']

    # Process the real-time data and extract relevant data points
    for timestamp, quote in realtime_data.items():
        symbol = 'AAPL'  # Replace with the actual symbol from the response
        open_price = quote['1. open']
        high_price = quote['2. high']
        low_price = quote['3. low']
        close_price = quote['4. close']
        volume = quote['5. volume']

        # Create a data structure to store the extracted data points
        data_point = {
            'symbol': symbol,
            'timestamp': timestamp,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume
        }

        

        # Do something with the extracted data, e.g., print it or store it in a database
        print(data_point)

else:
    print(f"Error: {response.status_code}")









# import requests
# import confluent_kafka
# import json

# # Replace 'YOUR_API_KEY' with your actual Alpha Vantage API key
# api_key = 'PTQ4550RUE322FB0'

# # Replace 'YOUR_BOOTSTRAP_SERVERS' with your Kafka bootstrap servers
# conf = {
#     'bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
#     'acks': 'all',
#     'retries': 3,
#     'linger.ms': 100,
#     'batch.num.messages': 10000
# }

# # Create a Kafka producer instance
# producer = confluent_kafka.Producer(conf)

# def produce_data(data):
#     producer.produce('stock-data', value=json.dumps(data))
#     producer.flush()

# # Define the endpoint and parameters for the API call
# url = 'https://www.alphavantage.co/query'
# params = {
#     'function': 'TIME_SERIES_INTRADAY',
#     'symbol': 'AAPL',  # Replace 'AAPL' with the desired stock symbol
#     'interval': '1min',  # Specify the desired time interval
#     'apikey': api_key
# }

# try:
#     # Make the API call
#     response = requests.get(url, params=params)

#     # Check if the API call was successful
#     if response.status_code == 200:
#         data = response.json()

#         # Extract the real-time stock data from the response
#         realtime_data = data['Time Series (1min)']

#         # Process the real-time data and extract relevant data points
#         for timestamp, quote in realtime_data.items():
#             symbol = 'AAPL'  # Replace with the actual symbol from the response
#             open_price = quote['1. open']
#             high_price = quote['2. high']
#             low_price = quote['3. low']
#             close_price = quote['4. close']
#             volume = quote['5. volume']

#             # Create a data structure to store the extracted data points
#             data_point = {
#                 'symbol': symbol,
#                 'timestamp': timestamp,
#                 'open': open_price,
#                 'high': high_price,
#                 'low': low_price,
#                 'close': close_price,
#                 'volume': volume
#             }

#             # Produce the data to Kafka
#             produce_data(data_point)
#     else:
#         print(f"Error: {response.status_code}")

# except Exception as e:
#     print(f"An error occurred: {e}")

# finally:
#     producer.close()