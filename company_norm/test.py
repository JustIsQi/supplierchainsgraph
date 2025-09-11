import os
from openai import AzureOpenAI, OpenAI

client = OpenAI(
    api_key="Bearer sk-9AiXl4JTI3FCPUIAkEh0Yw",
    base_url="http://10.102.0.61/v1",
)

result = client.chat.completions.parse(
    model="gpt-5-chat",
    messages=[
        {"role":"user","content":"特鲁利么啊joke"}
    ],
    temperature=1.0,
    max_completion_tokens=16384,

)

print(result.choices[0].message.content)


