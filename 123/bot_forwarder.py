import sys
import subprocess
import os
import pkg_resources

REQUIRED_PACKAGES = {
    "aiogram": ">=3.0.0,<4.0.0",
    "fastapi": "",
    "uvicorn": "",
    "jinja2": "",
    "python-multipart": ""
}

def ensure_package(package, version_spec):
    try:
        pkg_resources.require(f"{package}{version_spec}")
    except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict):
        print(f"Устанавливаю {package}{version_spec} ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", f"{package}{version_spec}"])

def ensure_all_packages():
    for pkg, ver in REQUIRED_PACKAGES.items():
        ensure_package(pkg, ver)

ensure_all_packages()
print("Все зависимости установлены. Запускаю бота...")

import asyncio
import json
import webbrowser
from datetime import datetime

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.client.bot import DefaultBotProperties

from fastapi import FastAPI, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import uvicorn

CONFIG_FILE = "forwarder_config.json"
LOG_FILE = "forwarder_forward.log"
BOT_TOKEN_FILE = "forwarder_bot_token.txt"
PANEL_PORT = 8010

def load_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "/add": {"source_chat_ids": [], "target_chat_id": None, "title": ""},
                "/error": {"source_chat_ids": [], "target_chat_id": None, "title": ""}
            }, f, ensure_ascii=False, indent=2)
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)

def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def load_token():
    if not os.path.exists(BOT_TOKEN_FILE):
        with open(BOT_TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write("PASTE_YOUR_BOT_TOKEN_HERE")
        print("Вставьте токен Telegram-бота в файл forwarder_bot_token.txt и перезапустите!")
        input("Нажмите Enter для выхода...")
        exit(1)
    with open(BOT_TOKEN_FILE, encoding="utf-8") as f:
        return f.read().strip()

def log_forward(command, from_chat, text, to_chat, status, error=""):
    log_entry = {
        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "command": command,
        "from_chat": from_chat,
        "to_chat": to_chat,
        "text": text,
        "status": status,
        "error": error
    }
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

def load_logs(limit=100):
    if not os.path.exists(LOG_FILE):
        return []
    with open(LOG_FILE, encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]
    return lines[-limit:][::-1]

app = FastAPI()
templates = Jinja2Templates(directory="templates_forwarder")

@app.get("/", response_class=HTMLResponse)
async def panel(request: Request):
    cfg = load_config()
    logs = load_logs()
    return templates.TemplateResponse("panel.html", {"request": request, "config": cfg, "logs": logs})

@app.post("/set_chat")
async def set_chat(
    command: str = Form(...),
    source_chat_ids: str = Form(...),
    target_chat_id: int = Form(...),
    title: str = Form("")
):
    cfg = load_config()
    if command not in cfg:
        cfg[command] = {}
    src_ids = [int(x) for x in source_chat_ids.split(",") if x.strip()]
    cfg[command]["source_chat_ids"] = src_ids
    cfg[command]["target_chat_id"] = target_chat_id
    cfg[command]["title"] = title
    save_config(cfg)
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/add_command")
async def add_command(command: str = Form(...)):
    command = command.strip().lower()
    if not command.startswith("/"):
        command = "/" + command
    cfg = load_config()
    if command not in cfg:
        cfg[command] = {"source_chat_ids": [], "target_chat_id": None, "title": ""}
        save_config(cfg)
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/delete_command")
async def delete_command(command: str = Form(...)):
    cfg = load_config()
    if command in cfg and command not in ["/add", "/error"]:
        del cfg[command]
        save_config(cfg)
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)

TOKEN = load_token()
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

@dp.message(F.chat.type.in_({"group", "supergroup", "private"}))
async def debug_and_forward(message: types.Message):
    print(f"[DEBUG] chat.id={message.chat.id} from={getattr(message.from_user,'id',None)} text={message.text}")
    config = load_config()
    if not message.text:
        return
    parts = message.text.strip().split(None, 1)
    command = parts[0].lower()
    for cmd, cdata in config.items():
        src_list = cdata.get("source_chat_ids", [])
        tgt = cdata.get("target_chat_id")
        if src_list and tgt and command == cmd and int(message.chat.id) in list(map(int, src_list)):
            text = parts[1] if len(parts) > 1 else ""
            try:
                if text:
                    to_send = f"{command} {text}"
                    await bot.send_message(tgt, to_send)
                    log_forward(command, message.chat.id, text, tgt, "success")
                    await message.reply("✅ Переслано!", reply=False)
                else:
                    await message.reply("После команды должен быть текст!", reply=False)
            except Exception as e:
                log_forward(command, message.chat.id, text, tgt, "fail", str(e))
                await message.reply(f"Ошибка пересылки: {e}", reply=False)

def open_panel_browser():
    url = f"http://127.0.0.1:{PANEL_PORT}/"
    try:
        webbrowser.open_new(url)
    except Exception as e:
        print(f"Ошибка открытия браузера: {e}")

async def main():
    loop = asyncio.get_event_loop()
    loop.call_later(1, open_panel_browser)
    runner = loop.create_task(bot_runner())
    config = uvicorn.Config(app, host="127.0.0.1", port=PANEL_PORT, log_level="info")
    server = uvicorn.Server(config)
    runner2 = loop.create_task(server.serve())
    await asyncio.gather(runner, runner2)

async def bot_runner():
    await dp.start_polling(bot)

if __name__ == "__main__":
    os.makedirs("templates_forwarder", exist_ok=True)
    if not os.path.exists("templates_forwarder/panel.html"):
        with open("templates_forwarder/panel.html", "w", encoding="utf-8") as f:
            f.write("<!-- Загрузите свежий шаблон! -->")
    asyncio.run(main())