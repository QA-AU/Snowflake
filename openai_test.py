from openai import OpenAI

client = OpenAI()  # API key auto-loaded from env or .env

response = client.responses.create(
    model="gpt-4o-mini",
    input="Explain Python virtual environments in simple terms."
)

print(response.output_text)
