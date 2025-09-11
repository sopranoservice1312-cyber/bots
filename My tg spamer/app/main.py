import os
import json
import asyncio
from datetime import datetime, timezone, timedelta

from .logging_setup import setup_logging
setup_logging()

import logging
logger = logging.getLogger(__name__)

from fastapi import FastAPI, Depends, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy import delete
from .database import Base, engine, get_db
from .models import Account, Template, MessageLog, Job
from .telethon_manager import telethon_manager
from .config import settings
from .sender import process_job
from .csv_utils import parse_csv

app = FastAPI(title="Telegram Consent Messenger — Pro")

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


def ensure_aware(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def safe_json_parse(raw: str | None):
    if not raw or not raw.strip():
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"Invalid JSON in global_ctx: {e}. Raw value: {raw!r}")
        return None


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        from sqlalchemy import text
        await conn.execute(text("ALTER TABLE message_logs ADD COLUMN IF NOT EXISTS peer_id BIGINT"))
        await conn.execute(text("ALTER TABLE message_logs ADD COLUMN IF NOT EXISTS access_hash BIGINT"))
    logger.info("Application startup complete. Launching scheduler.")
    asyncio.create_task(scheduler())


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, db: AsyncSession = Depends(get_db)):
    accounts = (await db.execute(select(Account))).scalars().all()
    templates_rows = (await db.execute(select(Template))).scalars().all()
    jobs = (await db.execute(select(Job).order_by(Job.id.desc()).limit(20))).scalars().all()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "accounts": accounts, "templates": templates_rows, "jobs": jobs}
    )


@app.get("/accounts", response_class=HTMLResponse)
async def accounts_page(request: Request, db: AsyncSession = Depends(get_db)):
    accounts = (await db.execute(select(Account))).scalars().all()
    return templates.TemplateResponse("accounts.html", {"request": request, "accounts": accounts})


@app.post("/accounts/add")
async def add_account(
    phone: str = Form(...),
    name: str | None = Form(None),
    api_id: int | None = Form(None),
    api_hash: str | None = Form(None),
    db: AsyncSession = Depends(get_db)
):
    session_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "sessions", f"{phone.replace('+','plus')}.session")
    )
    acc = Account(
        phone=phone,
        name=name,
        session_path=session_path,
        is_authorized=False,
        api_id=api_id,
        api_hash=api_hash
    )
    db.add(acc)
    await db.commit()
    logger.info(f"Account added: {phone}")
    return RedirectResponse("/accounts", status_code=303)


@app.post("/accounts/add_and_login")
async def add_and_login(
    phone: str = Form(...),
    name: str | None = Form(None),
    api_id: int | None = Form(None),
    api_hash: str | None = Form(None),
    db: AsyncSession = Depends(get_db)
):
    session_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "sessions", f"{phone.replace('+','plus')}.session")
    )
    acc = Account(
        phone=phone,
        name=name,
        session_path=session_path,
        is_authorized=False,
        api_id=api_id,
        api_hash=api_hash,
    )
    db.add(acc)
    await db.commit()
    phone_code_hash = await telethon_manager.start_login(acc.phone, api_id=api_id, api_hash=api_hash)
    acc.phone_code_hash = phone_code_hash
    await db.commit()
    logger.info(f"Account added and login started: {phone}")
    return RedirectResponse(f"/accounts/{acc.id}/login", status_code=303)


@app.post("/accounts/{acc_id}/delete")
async def delete_account(acc_id: int, db: AsyncSession = Depends(get_db)):
    acc = (await db.execute(select(Account).where(Account.id == acc_id))).scalars().first()
    if acc:
        try:
            if acc.session_path and os.path.exists(acc.session_path):
                os.remove(acc.session_path)
        except Exception as e:
            logger.exception(f"Failed to remove session file for account {acc_id}: {e}")
        await db.delete(acc)
        await db.commit()
        logger.info(f"Deleted account: {acc_id}")
    return RedirectResponse("/accounts", status_code=303)


@app.post("/accounts/{acc_id}/login/start")
async def login_start(acc_id: int, db: AsyncSession = Depends(get_db)):
    acc = (await db.execute(select(Account).where(Account.id == acc_id))).scalar_one()
    phone_code_hash = await telethon_manager.start_login(acc.phone, api_id=acc.api_id, api_hash=acc.api_hash)
    acc.phone_code_hash = phone_code_hash
    await db.commit()
    logger.info(f"Login started for account: {acc.phone}")
    return RedirectResponse(f"/accounts/{acc_id}/login", status_code=303)


@app.get("/accounts/{acc_id}/login", response_class=HTMLResponse)
async def login_code_page(request: Request, acc_id: int):
    return templates.TemplateResponse("login_code.html", {"request": request, "acc_id": acc_id})


@app.post("/accounts/{acc_id}/login/verify")
async def login_verify(acc_id: int, code: str = Form(...), password: str | None = Form(None), db: AsyncSession = Depends(get_db)):
    acc = (await db.execute(select(Account).where(Account.id == acc_id))).scalar_one()
    await telethon_manager.finalize_login(
        acc.phone,
        code=code,
        phone_code_hash=acc.phone_code_hash,
        password=password,
        api_id=acc.api_id,
        api_hash=acc.api_hash
    )
    acc.is_authorized = True
    acc.phone_code_hash = None
    await db.commit()
    logger.info(f"Login verified for account: {acc.phone}")
    return RedirectResponse("/accounts", status_code=303)


@app.get("/templates", response_class=HTMLResponse)
async def templates_page(request: Request, db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(Template))).scalars().all()
    return templates.TemplateResponse("templates.html", {"request": request, "rows": rows})


@app.post("/templates/add")
async def templates_add(title: str = Form(...), body: str = Form(...), db: AsyncSession = Depends(get_db)):
    db.add(Template(title=title, body=body))
    await db.commit()
    logger.info(f"Template added: {title}")
    return RedirectResponse("/templates", status_code=303)


@app.post("/templates/{tpl_id}/delete")
async def templates_delete(tpl_id: int, db: AsyncSession = Depends(get_db)):
    row = (await db.execute(select(Template).where(Template.id == tpl_id))).scalars().first()
    if row:
        await db.delete(row)
        await db.commit()
        logger.info(f"Template deleted: {tpl_id}")
    return RedirectResponse("/templates", status_code=303)


@app.get("/send", response_class=HTMLResponse)
async def send_page(request: Request, db: AsyncSession = Depends(get_db)):
    accounts = (await db.execute(select(Account).where(Account.is_authorized == True))).scalars().all()
    templates_rows = (await db.execute(select(Template))).scalars().all()
    return templates.TemplateResponse("send.html", {"request": request, "accounts": accounts, "templates": templates_rows})


@app.post("/send/dispatch")
async def send_dispatch(
    targets_text: str = Form(""),
    account_ids: list[int] = Form(...),
    template_ids: list[int] = Form(...),
    randomize: bool = Form(False),
    schedule_at: str | None = Form(None),
    global_ctx: str | None = Form(None),
    is_cyclic: bool = Form(False),
    cycle_minutes: str | None = Form(None),
    db: AsyncSession = Depends(get_db)
):
    if targets_text.strip():
        targets_blob = targets_text
    else:
        targets_blob = "[]"

    now = datetime.now(timezone.utc)
    sched_at = None
    if schedule_at:
        sched_at = datetime.fromisoformat(schedule_at)
        if sched_at.tzinfo is None:
            sched_at = sched_at.replace(tzinfo=timezone.utc)

    cycle_val = int(cycle_minutes) if is_cyclic and cycle_minutes and cycle_minutes.strip() else None

    job = Job(
        status="queued" if not schedule_at else "scheduled",
        account_id=None,
        targets_blob=targets_blob,
        template_ids_blob=json.dumps(template_ids),
        randomize=bool(randomize),
        schedule_at=sched_at,
        context_json=safe_json_parse(global_ctx),
        account_ids_blob=json.dumps(account_ids),
        is_cyclic=bool(is_cyclic),
        cycle_minutes=cycle_val,
        next_run_at=now + timedelta(minutes=cycle_val) if is_cyclic and cycle_val else None
    )
    db.add(job)
    await db.commit()
    logger.info(f"Send job dispatched: {job.id} status={job.status} schedule_at={job.schedule_at}")
    return RedirectResponse("/logs", status_code=303)


@app.post("/send/upload_csv")
async def upload_csv(
    file: UploadFile = File(...),
    account_ids: list[int] = Form(...),
    template_ids: list[int] = Form(...),
    randomize: bool = Form(False),
    schedule_at: str | None = Form(None),
    global_ctx: str | None = Form(None),
    is_cyclic: bool = Form(False),
    cycle_minutes: str | None = Form(None),
    db: AsyncSession = Depends(get_db)
):
    data = await file.read()
    pairs, cols = parse_csv(data)
    lines = []
    for target, ctx in pairs:
        lines.append(target + "\t" + json.dumps(ctx, ensure_ascii=False))

    now = datetime.now(timezone.utc)
    sched_at = None
    if schedule_at:
        sched_at = datetime.fromisoformat(schedule_at)
        if sched_at.tzinfo is None:
            sched_at = sched_at.replace(tzinfo=timezone.utc)

    cycle_val = int(cycle_minutes) if is_cyclic and cycle_minutes and cycle_minutes.strip() else None

    job = Job(
        status="queued" if not schedule_at else "scheduled",
        account_id=None,
        targets_blob="\n".join(lines),
        template_ids_blob=json.dumps(template_ids),
        randomize=bool(randomize),
        schedule_at=sched_at,
        context_json=safe_json_parse(global_ctx),
        account_ids_blob=json.dumps(account_ids),
        is_cyclic=bool(is_cyclic),
        cycle_minutes=cycle_val,
        next_run_at=now + timedelta(minutes=cycle_val) if is_cyclic and cycle_val else None
    )
    db.add(job)
    await db.commit()
    logger.info(f"Send job from CSV dispatched: {job.id} status={job.status} schedule_at={job.schedule_at}")
    return RedirectResponse("/logs", status_code=303)


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request, db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(MessageLog).order_by(MessageLog.id.desc()).limit(500))).scalars().all()
    accounts = {a.id: a for a in (await db.execute(select(Account))).scalars().all()}
    templates_rows = {t.id: t for t in (await db.execute(select(Template))).scalars().all()}
    jobs = (await db.execute(select(Job).order_by(Job.id.desc()).limit(50))).scalars().all()
    return templates.TemplateResponse("logs.html", {"request": request, "rows": rows, "accounts": accounts, "templates": templates_rows, "jobs": jobs})


@app.post("/jobs/{job_id}/pause")
async def pause_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one()
    job.paused = not job.paused
    await db.commit()
    logger.info(f"Job {job_id} paused={job.paused}")
    return RedirectResponse("/logs", status_code=303)


@app.post("/jobs/{job_id}/stop")
async def stop_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one()
    job.stopped = True
    await db.commit()
    logger.info(f"Job {job_id} stopped")
    return RedirectResponse("/logs", status_code=303)


@app.post("/jobs/{job_id}/delete")
async def delete_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = (await db.execute(select(Job).where(Job.id == job_id))).scalar_one_or_none()
    if job:
        await db.execute(delete(MessageLog).where(MessageLog.job_id == job_id))
        await db.delete(job)
        await db.commit()
        logger.info(f"Job {job_id} deleted permanently")
    return RedirectResponse("/logs", status_code=303)


async def scheduler():
    logger.info("Scheduler started")
    while True:
        try:
            async for db in get_db():
                jobs = (await db.execute(select(Job))).scalars().all()
                now = datetime.now(timezone.utc)
                logger.debug(f"Checking {len(jobs)} jobs at {now}")

                for job in jobs:
                    nr = ensure_aware(job.next_run_at)
                    sch = ensure_aware(job.schedule_at)
                    logger.debug(
                        f"Job {job.id} status={job.status} paused={job.paused} stopped={job.stopped} next_run_at={nr}"
                    )

                    if job.stopped or job.paused:
                        continue

                    if job.is_cyclic:
                        if nr and nr <= now:
                            logger.info(f"Launching cyclic job {job.id}")
                            job.status = "running"
                            await db.commit()
                            asyncio.create_task(process_job(job.id, cyclic=True))

                    elif job.status in ("queued", "scheduled"):
                        due = (sch is None) or (sch <= now)
                        if due:
                            logger.info(f"Launching job {job.id}")
                            job.status = "running"
                            await db.commit()
                            asyncio.create_task(process_job(job.id, cyclic=False))

                await asyncio.sleep(settings.SCHEDULER_TICK_SECONDS)

        except Exception as e:
            logger.exception("Exception in scheduler loop")
            await asyncio.sleep(2)


@app.get("/health")
async def health():
    return {"ok": True}
