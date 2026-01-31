#import files required
import time
import json
import requests
from kafka import KafkaProducer

#setup the variables for api
API_KEY = "d5u9qlhr01qtjet2h3igd5u9qlhr01qtjet2h3j0"  #api key
BASE_URL = "https://finnhub.io/api/v1/quote"            #url fomr which api recieved
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]     #5 companies data taken

#initial kafkaproducer
producer = KafkaProducer (
    bootstrap_servers = ["host.docker.internal:29092"],       #connecting to kafka. we can use normal localhost but we are running docker outside so taken entire link
    value_serializer = lambda v: json.dumps(v).encode("utf-8") #kafka streams the data in bytes. so we are converting the dictionary to json format then via utf-8 we are converting to bytes. 
)

#retreive data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"     #this is the whole url symbol, token api key coz we are gonna fetch it
    try:                                                    #doing exception handling inorder to fetch the api
        response = requests.get(url)        # get the url    
        response.raise_for_status()
        data = response.json()      #store it ni data in json format
        data["symbol"] = symbol         #create 2 metadata for each symbol and the time for each symbol. symbol basically company 
        data["fetch_at"] = int(time.time()) 
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}") #throw an error if not found
        return None

#looping and pushing to stream of the data
while True:
    for symbol in SYMBOLS:      #go through each company in SYMBOL array
        quote = fetch_quote(symbol)     #call the function for each symbol and return the symbol->(metadata)symbol and time
        if quote:
            print(f"Producing: {quote}")    #if its true then print it
            producer.send("stock-quotes", value=quote)  #then send it to kafka, where we created the topic in kafka stock-quotes
    time.sleep(6)       #the loop makes 5 requests per cycle that is one per symbol (symbol is company)