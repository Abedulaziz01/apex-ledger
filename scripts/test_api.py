import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

def run_api_probe() -> str:
    api_key = os.getenv("OPENROUTER_API_KEY")
    print("API key loaded:", bool(api_key))
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set")

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
    return response.choices[0].message.content

if __name__ == "__main__":
    print(run_api_probe())
