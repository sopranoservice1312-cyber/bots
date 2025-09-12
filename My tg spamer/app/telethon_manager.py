import os
import logging
import asyncio
import sqlite3
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from .config import settings

logger = logging.getLogger(__name__)

SESS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sessions"))
os.makedirs(SESS_DIR, exist_ok=True)


class TelethonManager:
    def __init__(self):
        self.clients: dict[str, TelegramClient] = {}
        self.locks: dict[str, asyncio.Lock] = {}

    def _session_path(self, phone: str) -> str:
        safe = phone.replace("+", "plus").replace(" ", "")
        return os.path.join(SESS_DIR, f"{safe}.session")

    def create_client(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> TelegramClient:
        path = self._session_path(phone)
        use_api_id = api_id or settings.TELEGRAM_API_ID
        use_api_hash = api_hash or settings.TELEGRAM_API_HASH
        return TelegramClient(path, use_api_id, use_api_hash)

    async def start_login(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> str:
        async with self._get_lock(phone):
            client = self.create_client(phone, api_id, api_hash)
            await client.connect()
            phone_code_hash = None
            if not await client.is_user_authorized():
                result = await client.send_code_request(phone)
                phone_code_hash = result.phone_code_hash
                logger.info(f"Sent login code to {phone}")
            else:
                logger.info(f"{phone} already authorized")
            await client.disconnect()
            return phone_code_hash

    async def finalize_login(
        self, phone: str, code: str, phone_code_hash: str,
        password: str | None = None, api_id: int | None = None, api_hash: str | None = None
    ) -> None:
        async with self._get_lock(phone):
            client = self.create_client(phone, api_id, api_hash)
            await client.connect()
            try:
                await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
            except SessionPasswordNeededError:
                if not password:
                    raise
                await client.sign_in(password=password)
            logger.info(f"Successfully logged in {phone}")
            await client.disconnect()

    async def ensure_connected(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> TelegramClient:
        async with self._get_lock(phone):
            client = self.clients.get(phone)

            # Если уже есть и соединён — возвращаем
            if client and client.is_connected():
                return client

            # Пробуем несколько раз
            for attempt in range(1, 4):
                try:
                    if client:
                        # reconnect
                        await client.connect()
                        if await client.is_user_authorized():
                            logger.info(f"Reconnected client {phone}")
                            return client

                    # новый клиент
                    client = self.create_client(phone, api_id, api_hash)
                    await client.connect()

                    if not await client.is_user_authorized():
                        raise RuntimeError(f"Account {phone} not authorized. Run login flow first.")

                    self.clients[phone] = client
                    logger.info(f"Client connected: {phone}")
                    return client

                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e).lower():
                        logger.warning(f"[{phone}] Database is locked, retry {attempt}/3...")
                        await asyncio.sleep(2)  # ждём и пробуем снова
                        continue
                    raise
                except Exception as e:
                    logger.error(f"Error connecting {phone}: {e}")
                    raise

            raise RuntimeError(f"Failed to connect {phone} after 3 attempts (database locked)")

    async def disconnect(self, client: TelegramClient):
        try:
            await client.disconnect()
            logger.info(f"Client {client.session.filename} disconnected")
        except Exception as e:
            logger.warning(f"Error while disconnecting: {e}")

    def _get_lock(self, phone: str) -> asyncio.Lock:
        if phone not in self.locks:
            self.locks[phone] = asyncio.Lock()
        return self.locks[phone]


telethon_manager = TelethonManager()
