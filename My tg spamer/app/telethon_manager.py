import os
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from .config import settings

logger = logging.getLogger(__name__)

SESS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sessions"))
os.makedirs(SESS_DIR, exist_ok=True)


class TelethonManager:
    def __init__(self):
        self.clients: dict[str, TelegramClient] = {}

    def _session_path(self, phone: str) -> str:
        """Формируем путь к .session файлу для конкретного номера"""
        safe = phone.replace("+", "plus").replace(" ", "")
        return os.path.join(SESS_DIR, f"{safe}.session")

    def create_client(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> TelegramClient:
        """Создать клиент, но не запускать"""
        path = self._session_path(phone)
        use_api_id = api_id or settings.TELEGRAM_API_ID
        use_api_hash = api_hash or settings.TELEGRAM_API_HASH
        return TelegramClient(path, use_api_id, use_api_hash)

    async def start_login(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> str:
        """
        Первый шаг входа — отправляем код.
        Возвращает phone_code_hash.
        """
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
        """
        Второй шаг входа — вводим код (и пароль 2FA при необходимости).
        """
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
        """Гарантируем подключение авторизованного клиента"""
        client = self.create_client(phone, api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(f"Account {phone} not authorized. Run login flow first.")
        logger.info(f"Client connected: {phone}")
        self.clients[phone] = client
        return client

    async def disconnect(self, client: TelegramClient):
        """Отключаем клиент"""
        try:
            await client.disconnect()
            logger.info(f"Client {client.session.filename} disconnected")
        except Exception as e:
            logger.warning(f"Error while disconnecting: {e}")


telethon_manager = TelethonManager()
