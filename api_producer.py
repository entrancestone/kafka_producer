
from kafka import KafkaProducer
import json
import http.client

def api_producer(country):
	producer =  KafkaProducer(bootstrap_servers=['localhost:9094'])
	topic = "api-spark1"


	conn = http.client.HTTPSConnection("covid-193.p.rapidapi.com")	

	headers = {
	    'x-rapidapi-host': "covid-193.p.rapidapi.com",
	    'x-rapidapi-key': "1dd7e0c489msh75cbcc0dfe48d63p1f0b96jsn9409f118d9a8"
	    }

	filtr = str("/history?country=" + country)

	req = conn.request("GET", filtr, headers=headers)

	res = conn.getresponse()
	json_data = res.read()


	data = json.loads(json_data.decode('utf-8'))

#	f_res = data["response"]

	print(json_data)
	print("Data read for " + str(data["response"][1]["country"]))
#	for i in f_res:
#		print("\n\nAt time of " + str(i["time"]) + "\n")
#		print("Details:")
	
#		for m, n in i["cases"].items():
#			print(m + ": " + str(n))
#		print("\nDeaths detail:")
#		for m, n in i["deaths"].items():
		
#			print(m + ": " + str(n))


	producer.send(topic, json_data)

if __name__ == '__main__':
	country = input("Which country do you want to look up?")
	api_producer(country)
