import asyncio, random, json
from datetime import datetime, timezone, timedelta
from telethon.errors import (
    FloodWaitError, UserPrivacyRestrictedError, ChatAdminRequiredError,
    PeerIdInvalidError, ChannelPrivateError, UsernameNotOccupiedError,
    InviteHashExpiredError, InviteHashInvalidError,
    UserAlreadyParticipantError
)
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import InputPeerUser, InputPeerChannel, InputPeerChat
from sqlalchemy import select
from .models import Account, Template, MessageLog, Job
from .telethon_manager import telethon_manager
from .utils import respectful_delay, render_placeholders
from .database import AsyncSessionLocal
import logging

logger = logging.getLogger(__name__)

# Глобальный кэш ограничений после FloodWait
flood_restrictions: dict[str, datetime] = {}


def normalize_target(raw: str) -> str:
    """Привести target к стандартному виду (username/ID/инвайт)"""
    t = raw.strip()
    if not t:
        return ""
    t = t.replace("https://", "").replace("http://", "")
    if t.startswith("t.me/"):
        t = t.split("t.me/")[-1]
    if t.startswith("@"):
        t = t[1:]
    return t


async def resolve_target(client, target: str):
    """Определение entity по username/ID/инвайту"""
    t = normalize_target(target)
    if not t:
        return None, "Empty target"

    # инвайт-ссылка
    if t.startswith("+"):
        try:
            invite = await client(CheckChatInviteRequest(t[1:]))
            if getattr(invite, "chat", None):
                try:
                    return await client(ImportChatInviteRequest(t[1:])), None
                except UserAlreadyParticipantError:
                    entity = await client.get_entity(invite.chat)
                    return entity, None
            return None, "Invite invalid or expired"
        except (InviteHashExpiredError, InviteHashInvalidError):
            return None, "Invite link expired/invalid"
        except Exception as e:
            return None, f"Invite error: {e}"

    if t.isdigit():
        try:
            return await client.get_entity(int(t)), None
        except Exception as e:
            return None, f"ID not found: {e}"

    # username / публичная ссылка
    try:
        entity = await client.get_entity(t)
        return entity, None
    except UsernameNotOccupiedError:
        return None, "Username not found"
    except ChannelPrivateError:
        return None, "Channel is private or no access"
    except Exception as e1:
        # fallback через get_input_entity
        try:
            entity = await client.get_input_entity(t)
            return entity, None
        except Exception as e2:
            return None, f"Target not found or no access: {e1} / fallback: {e2}"


async def get_entity_from_cache(client, log: MessageLog):
    """Восстановить entity из сохранённых peer_id/access_hash"""
    try:
        if log.peer_id and log.access_hash:
            if str(log.peer_id).startswith("-100"):  # супер-группа/канал
                return InputPeerChannel(channel_id=abs(int(log.peer_id)), access_hash=log.access_hash)
            else:
                return InputPeerUser(user_id=log.peer_id, access_hash=log.access_hash)
        elif log.peer_id:
            return InputPeerChat(chat_id=log.peer_id)
    except Exception as e:
        logger.warning(f"[get_entity_from_cache] Failed: {e}")
    return None


async def process_job(job_id: int, cyclic: bool = False):
    db = AsyncSessionLocal()
    try:
        job = await db.get(Job, job_id)
        if not job:
            logger.warning(f"Job {job_id} not found in DB")
            return

        logger.info(f"Starting job {job.id}, cyclic={cyclic}")

        # --- загрузка шаблонов
        templates = (
            await db.execute(
                select(Template).where(Template.id.in_(json.loads(job.template_ids_blob)))
            )
        ).scalars().all()
        if not templates:
            job.status, job.error = "failed", "No templates"
            await db.commit()
            return

        # --- цели
        try:
            targets = json.loads(job.targets_blob)
            if isinstance(targets, str):
                targets = [targets]
        except Exception:
            targets = [t.strip() for t in job.targets_blob.splitlines() if t.strip()]

        # нормализуем и убираем дубликаты
        targets = [normalize_target(t) for t in targets if t]
        targets = list(dict.fromkeys(targets))

        globals_ctx = job.context_json or {}

        # --- аккаунты
        try:
            account_ids = json.loads(job.account_ids_blob)
        except Exception:
            account_ids = []

        accs = (
            await db.execute(
                select(Account).where(
                    Account.id.in_(account_ids), Account.is_authorized.is_(True)
                )
            )
        ).scalars().all()
        if not accs:
            job.status, job.error = "failed", "No authorized accounts"
            await db.commit()
            return

        # --- запуск клиентов
        clients = {}
        try:
            for acc in accs:
                clients[acc.id] = await telethon_manager.ensure_connected(
                    acc.phone, acc.api_id, acc.api_hash
                )

            # --- рассылка
            for target in targets:
                acc = random.choice(accs)
                client = clients[acc.id]
                tpl = random.choice(templates) if job.randomize else templates[0]

                ctx = dict(globals_ctx or {})

                body = render_placeholders(tpl.body, ctx)

                log = MessageLog(
                    account_id=acc.id,
                    target=target,
                    template_id=tpl.id,
                    status="queued",
                )
                db.add(log)
                await db.flush()

                # flood-cache проверка
                now = datetime.now(timezone.utc)
                if target in flood_restrictions and now < flood_restrictions[target]:
                    log.status, log.error = "skipped", f"FloodWait active until {flood_restrictions[target]}"
                    logger.warning(f"[Job {job.id}] Skipped {target}: floodwait active")
                    await db.commit()
                    continue

                # --- пробуем взять entity
                entity = await get_entity_from_cache(client, log)
                error = None

                if not entity:
                    entity, error = await resolve_target(client, target)
                    if entity:
                        if hasattr(entity, "id"):
                            log.peer_id = entity.id
                        if hasattr(entity, "access_hash"):
                            log.access_hash = entity.access_hash

                if not entity:
                    log.status, log.error = "failed", error or "Target not found"
                    logger.warning(f"[Job {job.id}] Target {target} failed: {log.error}")
                    await db.commit()
                    continue

                # --- отправка
                await respectful_delay({}, key=str(acc.id))
                try:
                    logger.info(f"[Job {job.id}] Sending message to {target} via {acc.phone}")
                    await client.send_message(entity, body)
                    log.status, log.error = "sent", None
                    logger.info(f"[Job {job.id}] Message sent successfully to {target}")
                except FloodWaitError as e:
                    until = now + timedelta(seconds=e.seconds)
                    flood_restrictions[target] = until
                    log.status, log.error = "skipped", f"FloodWait {e.seconds}s (until {until})"
                    logger.warning(f"[Job {job.id}] FloodWait {e.seconds}s for {target}, skip until {until}")
                except (UserPrivacyRestrictedError, ChatAdminRequiredError, PeerIdInvalidError) as e:
                    log.status, log.error = "skipped", e.__class__.__name__
                    logger.warning(f"[Job {job.id}] Skipped {target}: {e.__class__.__name__}")
                except Exception as e:
                    log.status, log.error = "failed", str(e)
                    logger.error(f"[Job {job.id}] Failed to send to {target}: {e}")

                await db.commit()

        finally:
            for client in clients.values():
                await telethon_manager.disconnect(client)

        # --- обновление статуса
        if job.is_cyclic and not job.stopped:
            job.next_run_at = datetime.now(timezone.utc) + timedelta(
                minutes=job.cycle_minutes
            )
            job.status = "queued"
            logger.info(f"Job {job.id} re-queued for {job.next_run_at}")
        else:
            job.status = "finished"
            logger.info(f"Job {job.id} finished")

        await db.commit()

    except Exception as e:
        logger.error(f"[process_job] Fatal error: {e}", exc_info=True)
    finally:
        await db.close()
