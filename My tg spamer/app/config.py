import os

class Settings:
    # Можно задать свои значения по умолчанию или получать из env
    TELEGRAM_API_ID = int(os.getenv("TG_API_ID", "123456"))
    TELEGRAM_API_HASH = os.getenv("TG_API_HASH", "your_default_hash")
    SCHEDULER_TICK_SECONDS = 5

settings = Settings()