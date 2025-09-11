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
from .models import Account, Template, MessageLog, Job, FloodRestriction
from .telethon_manager import telethon_manager
from .utils import respectful_delay, render_placeholders
from .database import AsyncSessionLocal
import logging

logger = logging.getLogger(__name__)


def normalize_target(raw: str) -> str:
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
    t = normalize_target(target)
    if not t:
        return None, "Empty target"

    if t.startswith("+"):  # инвайт
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

    try:
        entity = await client.get_entity(t)
        return entity, None
    except UsernameNotOccupiedError:
        return None, "Username not found"
    except ChannelPrivateError:
        return None, "Channel is private or no access"
    except Exception as e1:
        try:
            entity = await client.get_input_entity(t)
            return entity, None
        except Exception as e2:
            return None, f"Target not found or no access: {e1} / fallback: {e2}"


async def get_entity_from_cache(client, log: MessageLog):
    try:
        if log.peer_id and log.access_hash:
            if str(log.peer_id).startswith("-100"):
                return InputPeerChannel(channel_id=abs(int(log.peer_id)), access_hash=log.access_hash)
            else:
                return InputPeerUser(user_id=log.peer_id, access_hash=log.access_hash)
        elif log.peer_id:
            return InputPeerChat(chat_id=log.peer_id)
    except Exception as e:
        logger.warning(f"[get_entity_from_cache] Failed: {e}")
    return None


# --- Flood restriction helpers ---
async def get_flood_until(account_id: int, peer_id: int | None):
    async with AsyncSessionLocal() as db:
        fr = await db.scalar(
            select(FloodRestriction).where(
                FloodRestriction.account_id == account_id,
                FloodRestriction.peer_id == (peer_id or 0)
            )
        )
        if fr and fr.until > datetime.now(timezone.utc):
            return fr.until
        return None


async def set_flood_until(account_id: int, peer_id: int | None, until):
    async with AsyncSessionLocal() as db:
        fr = await db.scalar(
            select(FloodRestriction).where(
                FloodRestriction.account_id == account_id,
                FloodRestriction.peer_id == (peer_id or 0)
            )
        )
        if fr:
            fr.until = until
        else:
            fr = FloodRestriction(
                account_id=account_id,
                peer_id=peer_id or 0,
                until=until
            )
            db.add(fr)
        await db.commit()


# --- Main job processor ---
async def process_job(job_id: int, cyclic: bool = False):
    db = AsyncSessionLocal()
    try:
        job = await db.get(Job, job_id)
        if not job:
            logger.warning(f"Job {job_id} not found in DB")
            return

        logger.info(f"Starting job {job.id}, cyclic={cyclic}")

        templates = (
            await db.execute(
                select(Template).where(Template.id.in_(json.loads(job.template_ids_blob)))
            )
        ).scalars().all()
        if not templates:
            job.status, job.error = "failed", "No templates"
            await db.commit()
            return

        try:
            targets = json.loads(job.targets_blob)
            if isinstance(targets, str):
                targets = [targets]
        except Exception:
            targets = [t.strip() for t in job.targets_blob.splitlines() if t.strip()]

        globals_ctx = job.context_json or {}

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

        clients = {}
        try:
            for acc in accs:
                clients[acc.id] = await telethon_manager.ensure_connected(
                    acc.phone, acc.api_id, acc.api_hash
                )

            for raw in targets:
                acc = random.choice(accs)
                client = clients[acc.id]
                tpl = random.choice(templates) if job.randomize else templates[0]

                ctx = dict(globals_ctx or {})
                if "\t" in raw:
                    raw_target, raw_ctx = raw.split("\t", 1)
                    ctx.update(json.loads(raw_ctx))
                    target = raw_target
                else:
                    target = raw

                norm_target = normalize_target(target)
                body = render_placeholders(tpl.body, ctx)

                log = MessageLog(
                    account_id=acc.id,
                    target=target,
                    template_id=tpl.id,
                    status="queued",
                )
                db.add(log)
                await db.flush()

                entity = await get_entity_from_cache(client, log)
                error = None

                if not entity:
                    entity, error = await resolve_target(client, norm_target)
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

                # --- flood check ---
                now = datetime.now(timezone.utc)
                until = await get_flood_until(acc.id, getattr(entity, "id", 0))
                if until and until > now:
                    log.status, log.error = "skipped", f"FloodWait until {until}"
                    logger.warning(f"[Job {job.id}] Skipped {target}, wait until {until}")
                    await db.commit()
                    continue

                await respectful_delay({}, key=str(acc.id))
                try:
                    logger.info(f"[Job {job.id}] Sending message to {target} via {acc.phone}")
                    await client.send_message(entity, body)
                    log.status, log.error = "sent", None
                    logger.info(f"[Job {job.id}] Message sent successfully to {target}")
                except FloodWaitError as e:
                    until = now + timedelta(seconds=e.seconds)
                    await set_flood_until(acc.id, getattr(entity, "id", 0), until)
                    log.status, log.error = "skipped", f"FloodWait {e.seconds}s"
                    logger.warning(f"[Job {job.id}] FloodWait {e.seconds}s on {acc.phone}, skipping until {until}")
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

        if job.is_cyclic and not job.stopped:
            job.next_run_at = datetime.now(timezone.utc) + timedelta(minutes=job.cycle_minutes)
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
