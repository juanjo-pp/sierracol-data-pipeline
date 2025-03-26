import requests
import json

url = "https://hooks.slack.com/services/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"
headers = {"Content-Type": "application/json"}
data = {"text": "Hello, World!"}

response = requests.post(url, headers=headers, data=json.dumps(data))

print(response.status_code)
print(response.text)