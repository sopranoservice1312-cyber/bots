import os

class Settings:
    # Можно задать свои значения по умолчанию или получать из env
    TELEGRAM_API_ID = int(os.getenv("TG_API_ID", "29626149"))
    TELEGRAM_API_HASH = os.getenv("TG_API_HASH", "c95627c11e62cba634e03fff562494e2")
    SCHEDULER_TICK_SECONDS = 5

settings = Settings()
