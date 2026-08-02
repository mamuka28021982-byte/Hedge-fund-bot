import os
import logging
import yfinance as yf
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ლოგირების კონფიგურაცია
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# სრული დაფარვის სია თქვენი მოთხოვნის მიხედვით
STOCKS = {
    "🏛 BIG TECH & AI GIANTS": ["NVDA", "MSFT", "GOOGL", "AAPL", "INTC"],
    "🤖 AI GROWTH & ROBOTICS": ["PLTR", "SOUN", "BBAI"],
    "⚛️ QUANTUM COMPUTING": ["IBM", "IONQ", "RGTI", "QBTS", "HON", "QNT-USD"],
    "🚀 SPACE & DEFENSE (SpaceX Exposure)": ["DXYZ", "RKLB", "LMT", "NOC", "KTOS"]
}

def get_stock_data(ticker):
    """საბაზრო მონაცემების, ტექნიკური ინდიკატორებისა და რისკების გამოთვლა"""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="1mo")
        if df.empty or len(df) < 2:
            return None
        
        current_price = df['Close'].iloc[-1]
        prev_price = df['Close'].iloc[-2]
        change_pct = ((current_price - prev_price) / prev_price) * 100
        
        # RSI (14) გამოთვლა
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        
        if not loss.empty and not gain.empty and loss.iloc[-1] != 0:
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs.iloc[-1]))
        else:
            rsi = 50.0
            
        volatility = df['Close'].pct_change().std() * (252 ** 0.5) * 100
        
        # სიგნალებისა და რისკის მართვის ლოგიკა
        if rsi > 70:
            signal = "🚨 SELL / TAKE PROFIT"
            risk = "🔴 მაღალი რისკი (High Risk)"
        elif rsi < 35:
            signal = "🟢 BUY / ACCUMULATE"
            risk = "🟢 დაბალი / ზომიერი რისკი"
        else:
            signal = "🟡 HOLD / NEUTRAL"
            risk = "🟡 საშუალო რისკი"
            
        return {
            "price": f"${current_price:.2f}",
            "change": f"{change_pct:+.2f}%",
            "rsi": f"{rsi:.1f}",
            "volatility": f"{volatility:.1f}%",
            "risk": risk,
            "signal": signal
        }
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")
        return None

async def send_analytics(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """ანალიტიკის გენერაცია და გაგზავნა ტელეგრამის ლიმიტების დაცვით"""
    header = "🏛 **INSTITUTIONAL HEDGE FUND ANALYTICS**\n⏱ დრო: რეალურ დროში (Real-time)\n----------------------------------"
    messages = [header]
    
    current_chunk = ""
    
    for category, tickers in STOCKS.items():
        cat_header = f"\n\n━━━━━ {category} ━━━━━\n"
        
        tickers_text = ""
        for ticker in tickers:
            data = get_stock_data(ticker)
            clean_name = ticker.replace("-USD", "")
            if data:
                tickers_text += f"\n▪ **{clean_name}**\n  ├ ფასი: {data['price']} ({data['change']})\n  ├ RSI (14): {data['rsi']} | ვოლატილობა: {data['volatility']}\n  ├ რისკი: {data['risk']}\n  └ სიგნალი: {data['signal']}\n"
            else:
                tickers_text += f"\n⚠️ **{clean_name}**: მონაცემების მიღების შეცდომა\n"
                
        block = cat_header + tickers_text
        
        # ტელეგრამის 4096 სიმბოლოს ლიმიტის დაცვა (ვყოფთ ბლოკებად)
        if len(current_chunk) + len(block) > 3500:
            messages.append(current_chunk)
            current_chunk = block
        else:
            current_chunk += block
            
    if current_chunk:
        messages.append(current_chunk)
        
    footer = "\n\n🎯 **ჰეჯ-ფონდის სტრატეგიული რეზიუმე:**\nბაზარი იმყოფება დინამიკურ ფაზაში. მკაცრად აკონტროლეთ პოზიციები და რისკები."
    messages.append(footer)

    for msg in messages:
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🏛 **Mamuka AI Hedge Fund Bot აქტიურია!**\n\n"
        "ბოტი მუშაობს 24/7 რეჟიმში.\n"
        "ბრძანებები:\n"
        "• `/analyze` - სრული საბაზრო ანალიტიკის მყისიერად მიღება\n"
        "• `/status` - ბოტის აქტივობის შემოწმება"
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ სისტემა მუშაობს იდეალურად. სერვერი ფხიზელ რეჟიმშია!")

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ მიმდინარეობს ბაზრის მონაცემების, სტრუქტურებისა და სიგნალების ანალიზი...")
    await send_analytics(context, update.effective_chat.id)

if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("analyze", analyze))
    
    print("Bot is running...")
    app.run_polling()
