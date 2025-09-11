import random
import asyncio
import re


async def respectful_delay(counter: dict, key: str = "default", min_secs: float = 1.2, max_secs: float = 2.5):
    """
    Пауза между действиями, чтобы имитировать "человека".
    """
    delay = random.uniform(min_secs, max_secs)
    await asyncio.sleep(delay)


def render_placeholders(text: str, ctx: dict) -> str:
    """
    Подстановка {{key}} -> ctx[key].
    Если ключа нет — оставляем как есть.
    """
    def repl(match):
        key = match.group(1)
        return str(ctx.get(key, match.group(0)))

    return re.sub(r"\{\{(\w+)\}\}", repl, text)
