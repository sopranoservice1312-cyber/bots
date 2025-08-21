import random
import asyncio

async def respectful_delay(counter: dict, key: str = "default", min_secs: float = 1.2, max_secs: float = 2.5):
    delay = random.uniform(min_secs, max_secs)
    await asyncio.sleep(delay)

def render_placeholders(text: str, ctx: dict) -> str:
    # Примитивная подстановка {{key}} -> ctx[key]
    for k, v in ctx.items():
        text = text.replace("{{" + k + "}}", str(v))
    return text