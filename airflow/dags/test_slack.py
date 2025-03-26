import requests
import json

url = "https://hooks.slack.com/services/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"
headers = {"Content-Type": "application/json"}
data = {"text": "Hello, World!"}

response = requests.post(url, headers=headers, data=json.dumps(data))

print(response.status_code)
print(response.text)