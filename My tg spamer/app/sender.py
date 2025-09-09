import asyncio, random, json
from datetime import datetime, timezone, timedelta
from telethon.errors import FloodWaitError, UserPrivacyRestrictedError, ChatAdminRequiredError, PeerIdInvalidError
from sqlalchemy import select
from .models import Account, Template, MessageLog, Job
from .telethon_manager import telethon_manager
from .utils import respectful_delay, render_placeholders
from .database import AsyncSessionLocal

MAX_RETRIES = 3

async def resolve_target(client, target: str):
    t = target.strip()
    if not t:
        return None
    if t.startswith("http"):
        t = t.split("/")[-1]
    if t.startswith("@"):
        t = t[1:]
    try:
        entity = await client.get_entity(t)
        return entity
    except Exception:
        try:
            entity = await client.get_entity(int(t)) if t.isdigit() else None
            return entity
        except Exception:
            return None

async def process_job(job_id: int, cyclic=False):
    async with AsyncSessionLocal() as db:
        job = await db.get(Job, job_id)
        if not job:
            return

        templates = (await db.execute(
            select(Template).where(Template.id.in_(json.loads(job.template_ids_blob)))
        )).scalars().all()
        if not templates:
            job.status = "failed"
            job.error = "No templates"
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

        if not account_ids:
            job.status = "failed"
            job.error = "No accounts selected"
            await db.commit()
            return

        accs = (await db.execute(
            select(Account).where(Account.id.in_(account_ids), Account.is_authorized==True)
        )).scalars().all()
        if not accs:
            job.status = "failed"
            job.error = "No authorized accounts selected"
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
                body = render_placeholders(tpl.body, ctx)

                log = MessageLog(account_id=acc.id, target=target, template_id=tpl.id, status="queued")
                db.add(log)
                await db.flush()

                entity = await resolve_target(client, target)
                if not entity:
                    log.status = "failed"
                    log.error = "Target not found"
                    await db.commit()
                    continue

                await respectful_delay({}, key=str(acc.id))
                try:
                    await client.send_message(entity, body)
                    log.status = "sent"
                    log.error = None
                except FloodWaitError as e:
                    log.status = "retried"
                    log.error = f"FloodWait: wait {e.seconds}s"
                    await db.commit()
                    await asyncio.sleep(min(e.seconds + 1, 3600))
                    try:
                        await client.send_message(entity, body)
                        log.status = "sent"
                        log.error = None
                    except Exception as e2:
                        log.status = "failed"
                        log.error = str(e2)
                except (UserPrivacyRestrictedError, ChatAdminRequiredError, PeerIdInvalidError) as e:
                    log.status = "skipped"
                    log.error = e.__class__.__name__
                except Exception as e:
                    log.status = "failed"
                    log.error = str(e)
                await db.commit()
        finally:
            for client in clients.values():
                await telethon_manager.disconnect(client)

        if job.is_cyclic and not job.stopped:
            job.next_run_at = datetime.now(timezone.utc) + timedelta(minutes=job.cycle_minutes)
            job.status = "queued"
            await db.commit()
        else:
            job.status = "finished"
            await db.commit()
