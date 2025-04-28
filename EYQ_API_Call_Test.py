import requests
import os

# More secure way
# incubator_endpoint = os.getenv("EYQ_INCUBATOR_ENDPOINT")
# incubator_key = os.getenv("EYQ_INCUBATOR_KEY")

# Less secure way
incubator_endpoint = "test-incubator.openai.azure.com"
incubator_key = "test-key"  # Replace with your actual key

model = "gpt-4-turbo"  # Replace with desired model

api_version = "2024-06-01"

body = {
    "messages":[
        {
            "role":"user","content":"Am calling you via API, Testing?."
        }
    ]
}

headers = {
    "api-key": incubator_key
}

query_params = {
    "api-version": api_version
}

full_path = incubator_endpoint + "/openai/deployments/" + model + "/chat/completions"
#/openai/deployments/gpt-4-turbo/chat/completions 
response = requests.post(full_path, json=body, headers=headers, params=query_params)

status_code = response.status_code

response = response.json()

if status_code == 200:
    print()
    print(response["choices"][0]["message"]["content"])
    print()
else:
    print()
    print("Error: ", status_code)
    print("Response: ", response["error"])
    print()