# Telegram Queue & Post Broadcasting Bot

Built using **Python 3.10+**, **aiogram 3.x**, and **aiosqlite**.

---

### Where to Update Keys:

All tokens, secrets, and configurations are stored in the `.env` file (or Railway Environment Variables).

1. **Telegram Bot API Key**:
   - Variable name: `BOT_TOKEN`
   - Value: Get it from [@BotFather](https://t.me/BotFather) on Telegram.
2. **Admin User ID**:
   - Variable name: `ADMIN_ID`
   - Value: Your numerical ID from [@userinfobot](https://t.me/userinfobot).
3. **Database API Key / Connection**:
   - This bot uses **SQLite (`aiosqlite`)**, an asynchronous embedded database engine.
   - **No external Database API key or cloud credentials are required.** It saves data automatically into `bot_data.db`.
   - On **Railway**, attach a Persistent Volume pointing to `/app` (or configure `DATABASE_PATH=/path/to/volume/bot_data.db`) so your data persists across deployments.

---

### Project Structure:
- `main.py` - Static engine: Initializes aiogram Bot & Dispatcher, runs polling.
- `features.py` - Dynamic workspace: All handlers, queues, stats, FSM, and broadcast logic.
- `requirements.txt` - Python dependencies.
- `.env` / `.env.example` - Configuration settings.
- `.gitignore` - Prevents sensitive tokens & database files from reaching Git.
