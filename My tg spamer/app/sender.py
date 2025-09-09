import asyncio, random, json
from datetime import datetime, timezone, timedelta
from telethon.errors import (
    FloodWaitError, UserPrivacyRestrictedError,
    ChatAdminRequiredError, PeerIdInvalidError
)
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest
from sqlalchemy import select
from .models import Account, Template, MessageLog, Job
from .telethon_manager import telethon_manager
from .utils import respectful_delay, render_placeholders
from .database import AsyncSessionLocal
import logging

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


async def resolve_target(client, target: str, log: MessageLog = None, db=None):
    raw = target.strip()
    logger.debug(f"[resolve_target] Raw target: {raw}")
    if not raw:
        return None

    t = raw.replace("https://", "").replace("http://", "")
    if t.startswith("t.me/"):
        t = t.split("t.me/")[-1]

    # invite-ссылка
    if t.startswith("+"):
        try:
            logger.debug(f"[resolve_target] CheckChatInviteRequest({t[1:]})")
            invite = await client(CheckChatInviteRequest(t[1:]))

            if hasattr(invite, "chat"):
                logger.info(f"[resolve_target] Invite valid: {invite.chat.title} ({t})")
            else:
                logger.warning(f"[resolve_target] Invite invalid or revoked: {t}")
                if log and db:
                    log.status, log.error = "failed", "Invite invalid or revoked"
                    await db.commit()
                return None

            logger.debug(f"[resolve_target] ImportChatInviteRequest({t[1:]})")
            return await client(ImportChatInviteRequest(t[1:]))
        except Exception as e:
            logger.warning(f"[resolve_target] Invite link error for {t}: {e}")
            if log and db:
                log.status, log.error = "failed", f"Invite error: {e}"
                await db.commit()
            return None

    # username
    if t.startswith("@"):
        t = t[1:]

    # username или ID
    try:
        logger.debug(f"[resolve_target] Trying get_entity({t})")
        return await client.get_entity(t)
    except Exception as e1:
        logger.warning(f"[resolve_target] get_entity({t}) failed: {e1}")

        if isinstance(t, str) and not t.isdigit():
            try:
                logger.debug(f"[resolve_target] Trying JoinChannelRequest({t})")
                await client(JoinChannelRequest(t))
                entity = await client.get_entity(t)
                logger.info(f"[resolve_target] Successfully joined {t}")
                return entity
            except Exception as e2:
                logger.error(f"[resolve_target] JoinChannelRequest failed for {t}: {e2}")
                if log and db:
                    log.status, log.error = "failed", f"JoinChannel error: {e2}"
                    await db.commit()
                return None

        if t.isdigit():
            try:
                return await client.get_entity(int(t))
            except Exception as e3:
                logger.error(f"[resolve_target] get_entity(int) failed for {t}: {e3}")
                if log and db:
                    log.status, log.error = "failed", f"Invalid ID: {e3}"
                    await db.commit()
                return None

    return None


async def process_job(job_id: int, cyclic: bool = False):
    async with AsyncSessionLocal() as db:
        job = await db.get(Job, job_id)
        if not job:
            logger.warning(f"Job {job_id} not found in DB")
            return

        logger.info(f"Starting job {job.id}, cyclic={cyclic}")

        # --- шаблоны
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

        globals_ctx = job.context_json or {}

        # --- аккаунты
        try:
            account_ids = json.loads(job.account_ids_blob)
        except Exception:
            account_ids = []

        if not account_ids:
            job.status, job.error = "failed", "No accounts selected"
            await db.commit()
            return

        accs = (
            await db.execute(
                select(Account).where(
                    Account.id.in_(account_ids), Account.is_authorized.is_(True)
                )
            )
        ).scalars().all()
        if not accs:
            job.status, job.error = "failed", "No authorized accounts selected"
            await db.commit()
            return

        # --- клиенты
        clients = {}
        try:
            for acc in accs:
                clients[acc.id] = await telethon_manager.ensure_connected(
                    acc.phone, acc.api_id, acc.api_hash
                )

            # --- рассылка
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

                body = render_placeholders(tpl.body, ctx)

                log = MessageLog(
                    account_id=acc.id,
                    target=target,
                    template_id=tpl.id,
                    status="queued",
                )
                db.add(log)
                await db.flush()

                entity = await resolve_target(client, target, log, db)
                if not entity:
                    if log.status == "queued":  # если ещё не обновили
                        log.status, log.error = "failed", "Target not found or no access"
                        await db.commit()
                    continue

                await respectful_delay({}, key=str(acc.id))

                try:
                    await client.send_message(entity, body)
                    log.status, log.error = "sent", None
                except FloodWaitError as e:
                    log.status, log.error = "retried", f"FloodWait: wait {e.seconds}s"
                    await db.commit()
                    await asyncio.sleep(min(e.seconds + 1, 3600))
                    try:
                        await client.send_message(entity, body)
                        log.status, log.error = "sent", None
                    except Exception as e2:
                        log.status, log.error = "failed", str(e2)
                except (UserPrivacyRestrictedError, ChatAdminRequiredError, PeerIdInvalidError) as e:
                    log.status, log.error = "skipped", e.__class__.__name__
                except Exception as e:
                    log.status, log.error = "failed", str(e)

                await db.commit()

        finally:
            for client in clients.values():
                await telethon_manager.disconnect(client)

        # --- статус задачи
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
