import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from .config import settings

SESS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sessions"))
os.makedirs(SESS_DIR, exist_ok=True)

class TelethonManager:
    def __init__(self):
        self.clients = {}

    def _session_path(self, phone: str) -> str:
        safe = phone.replace("+", "plus").replace(" ", "")
        return os.path.join(SESS_DIR, f"{safe}.session")

    def create_client(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> TelegramClient:
        path = self._session_path(phone)
        use_api_id = api_id if api_id else settings.TELEGRAM_API_ID
        use_api_hash = api_hash if api_hash else settings.TELEGRAM_API_HASH
        client = TelegramClient(path, use_api_id, use_api_hash)
        return client

    async def start_login(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> str:
        client = self.create_client(phone, api_id, api_hash)
        await client.connect()
        phone_code_hash = None
        if not await client.is_user_authorized():
            result = await client.send_code_request(phone)
            phone_code_hash = result.phone_code_hash
        await client.disconnect()
        return phone_code_hash

    async def finalize_login(self, phone: str, code: str, phone_code_hash: str, password: str | None = None, api_id: int | None = None, api_hash: str | None = None) -> None:
        client = self.create_client(phone, api_id, api_hash)
        await client.connect()
        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        except SessionPasswordNeededError:
            if not password:
                raise
            await client.sign_in(password=password)
        finally:
            await client.disconnect()

    async def ensure_connected(self, phone: str, api_id: int | None = None, api_hash: str | None = None) -> TelegramClient:
        client = self.create_client(phone, api_id, api_hash)
        await client.connect()
        return client

    async def disconnect(self, client: TelegramClient):
        await client.disconnect()

telethon_manager = TelethonManager()