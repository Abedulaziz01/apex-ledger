import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

api_key = os.getenv("OPENROUTER_API_KEY")
print("API key loaded:", bool(api_key))

client = OpenAI(
    api_key=api_key,
    base_url="https://openrouter.ai/api/v1"
)

response = client.chat.completions.create(
    model="openai/gpt-4.1-mini",  # important: prefix with openai/
    messages=[
        {"role": "user", "content": "Reply only with: API key works"}
    ],
    max_tokens=20,
)

print(response.choices[0].message.content)