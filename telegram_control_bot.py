import os
import asyncio
import logging
import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def load_secrets():
    secrets_path = os.path.expanduser("~/.claude/ai-secrets.env")
    if os.path.exists(secrets_path):
        with open(secrets_path, "r") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value.strip().strip('"').strip("'")

load_secrets()

TOKEN = os.getenv("TELEGRAM_CONTROL_BOT_TOKEN")
ALLOWED_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://localhost:5050").rstrip("/")

def is_authorized(update: Update):
    return str(update.effective_chat.id) == str(ALLOWED_CHAT_ID)

async def get_main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Status", callback_data='status'), InlineKeyboardButton("🔄 Refresh", callback_data='status')],
        [InlineKeyboardButton("🔍 Scrape Leads", callback_data='scrape')],
        [InlineKeyboardButton("✅ Approve & Send", callback_data='approve_menu')],
        [InlineKeyboardButton("⏹ Stop Pipeline", callback_data='stop')]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("Unauthorized.")
        return
    await update.message.reply_text("🚀 Outreach Dashboard Control", reply_markup=await get_main_menu_keyboard())

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not is_authorized(update):
        return

    data = query.data

    async with httpx.AsyncClient() as client:
        try:
            if data == 'status':
                response = await client.get(f"{DASHBOARD_URL}/api/stats")
                if response.status_code == 200:
                    s = response.json()
                    text = (f"📊 *Pipeline Status*\n\n"
                            f"Total Sent: {s.get('total_sent', 0)}\n"
                            f"Sent Today: {s.get('sent_today', 0)}\n"
                            f"Pending: {s.get('pending', 0)}\n"
                            f"Stage: {s.get('pipeline_stage', 'Unknown')}")
                else:
                    text = f"❌ Error fetching stats: {response.status_code}"
                
                keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='menu')]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

            elif data == 'scrape':
                response = await client.post(f"{DASHBOARD_URL}/api/pipeline/scrape")
                text = "✅ Lead scraping initiated!" if response.status_code in [200, 201, 202] else f"❌ Failed to start scraping: {response.text}"
                keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='menu')]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

            elif data == 'approve_menu':
                keyboard = [
                    [InlineKeyboardButton("💼 Professional", callback_data='send_professional')],
                    [InlineKeyboardButton("☕ Casual", callback_data='send_casual')],
                    [InlineKeyboardButton("📖 Story-led", callback_data='send_story')],
                    [InlineKeyboardButton("🎯 Pain-first", callback_data='send_pain')],
                    [InlineKeyboardButton("⚡ Direct", callback_data='send_direct')],
                    [InlineKeyboardButton("🔙 Back", callback_data='menu')]
                ]
                await query.edit_message_text("Select Email Format to Approve & Send:", reply_markup=InlineKeyboardMarkup(keyboard))

            elif data.startswith('send_'):
                format_id = data.replace('send_', '')
                response = await client.post(f"{DASHBOARD_URL}/api/approve-batch", json={"format_id": format_id})
                text = f"✅ Approved and sending batch with format: *{format_id.capitalize()}*" if response.status_code in [200, 202] else f"❌ Error: {response.text}"
                keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='menu')]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

            elif data == 'stop':
                response = await client.post(f"{DASHBOARD_URL}/api/pipeline/stop")
                text = "🛑 Pipeline stopped successfully." if response.status_code == 200 else f"❌ Error stopping pipeline: {response.text}"
                keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='menu')]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

            elif data == 'menu':
                await query.edit_message_text("🚀 Outreach Dashboard Control", reply_markup=await get_main_menu_keyboard())

        except Exception as e:
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='menu')]]
            await query.edit_message_text(f"⚠️ Connection Error: {str(e)}", reply_markup=InlineKeyboardMarkup(keyboard))

if __name__ == '__main__':
    if not TOKEN:
        print("Error: TELEGRAM_CONTROL_BOT_TOKEN not found in env or secrets file.")
        exit(1)
        
    application = ApplicationBuilder().token(TOKEN).build()
    
    application.add_handler(CommandHandler(["start", "menu"], start))
    application.add_handler(CallbackQueryHandler(handle_buttons))
    
    print("Bot is running...")
    application.run_polling()
