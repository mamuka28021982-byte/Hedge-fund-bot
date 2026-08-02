import os
import logging
import threading
from flask import Flask
import yfinance as yf
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from deep_translator import GoogleTranslator

# ლოგირების კონფიგურაცია
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# სრული დაფარვის სია
STOCKS = {
    "🏛 BIG TECH & AI GIANTS": ["NVDA", "MSFT", "GOOGL", "AAPL", "INTC"],
    "🤖 AI GROWTH & ROBOTICS": ["PLTR", "SOUN", "BBAI"],
    "⚛️ QUANTUM COMPUTING": ["IBM", "IONQ", "RGTI", "QBTS", "HON", "QNT-USD"],
    "🚀 SPACE & DEFENSE (SpaceX Exposure)": ["DXYZ", "RKLB", "LMT", "NOC", "KTOS"]
}

def get_stock_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="1mo")
        if df is None or df.empty or len(df) < 2:
            return None
        
        current_price = float(df['Close'].iloc[-1])
        prev_price = float(df['Close'].iloc[-2])
        change_pct = ((current_price - prev_price) / prev_price) * 100
        
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        
        if not loss.empty and not gain.empty and len(loss) > 0 and loss.iloc[-1] != 0:
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs.iloc[-1]))
        else:
            rsi = 50.0
            
        volatility = float(df['Close'].pct_change().std() * (252 ** 0.5) * 100)
        
        if rsi > 70:
            signal = "🚨 SELL / TAKE PROFIT"
            risk = "🔴 მაღალი რისკი"
        elif rsi < 35:
            signal = "🟢 BUY / ACCUMULATE"
            risk = "🟢 დაბალი რისკი"
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

def get_stock_news(ticker):
    try:
        stock = yf.Ticker(ticker)
        news_list = stock.news
        if news_list and len(news_list) > 0:
            latest = news_list[0]
            title = latest.get('title') or latest.get('content', {}).get('title', 'სიახლე არ მოიძებნა')
            link = ""
            if 'link' in latest:
                link = latest['link']
            elif 'content' in latest and 'clickThroughUrl' in latest['content']:
                link = latest['content']['clickThroughUrl'].get('url', '')
            
            # ავტომატურად ვთარგმნით სათაურს ქართულად
            try:
                translated_title = GoogleTranslator(source='en', target='ka').translate(title)
                if translated_title:
                    title = translated_title
            except Exception as trans_err:
                logger.error(f"Translation error: {trans_err}")

            return {"title": title, "link": link}
    except Exception as e:
        logger.error(f"Error fetching news for {ticker}: {e}")
    return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text(
            "🏛 **Mamuka AI Hedge Fund Bot აქტიურია!**\n\n"
            "ბრძანებები:\n"
            "• `/analyze` - სრული საბაზრო ანალიტიკა\n"
            "• `/news` - უახლესი საბაზრო სიახლეები (ქართულად)\n"
            "• `/status` - სისტემის შემოწმება"
        )
    except Exception as e:
        logger.error(f"Start error: {e}")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("✅ სისტემა მუშაობს იდეალურად!")
    except Exception as e:
        logger.error(f"Status error: {e}")

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("⏳ მიმდინარეობს ბაზრის მონაცემების დამუშავება...")
        
        header = "🏛 **INSTITUTIONAL HEDGE FUND ANALYTICS**\n----------------------------------"
        messages = [header]
        current_chunk = ""
        
        for category, tickers in STOCKS.items():
            cat_header = f"\n\n━━━━━ {category} ━━━━━\n"
            tickers_text = ""
            for ticker in tickers:
                data = get_stock_data(ticker)
                clean_name = ticker.replace("-USD", "")
                if data:
                    tickers_text += f"\n▪ **{clean_name}**\n  ├ ფასი: {data['price']} ({data['change']})\n  ├ RSI: {data['rsi']} | ვოლატილობა: {data['volatility']}\n  ├ რისკი: {data['risk']}\n  └ სიგნალი: {data['signal']}\n"
                else:
                    tickers_text += f"\n⚠️ **{clean_name}**: მონაცემები მიუწვდომელია\n"
                    
            block = cat_header + tickers_text
            if len(current_chunk) + len(block) > 3500:
                messages.append(current_chunk)
                current_chunk = block
            else:
                current_chunk += block
                
        if current_chunk:
            messages.append(current_chunk)
            
        footer = "\n\n🎯 **რეზიუმე:** აკონტროლეთ რისკები."
        messages.append(footer)

        for msg in messages:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=msg, parse_mode="Markdown")
            
    except Exception as e:
        logger.error(f"Analyze error: {e}")
        await update.message.reply_text(f"⚠️ შეცდომა ანალიზის დროს: {str(e)}")

async def news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("⏳ ვეძებ და ვთარგმნი უახლეს სიახლეებს ქართულად...")
        
        key_tickers = ["NVDA", "PLTR", "IBM", "MSFT", "RKLB"]
        text = "📰 **მნიშვნელოვანი საბაზრო და ტექნოლოგიური სიახლეები**\n----------------------------------"
        
        for ticker in key_tickers:
            news_item = get_stock_news(ticker)
            if news_item:
                if news_item['link']:
                    text += f"\n\n▪ **{ticker}**\n🔗 [{news_item['title']}]({news_item['link']})"
                else:
                    text += f"\n\n▪ **{ticker}**\n📌 {news_item['title']}"
                    
        await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
        
    except Exception as e:
        logger.error(f"News error: {e}")
        await update.message.reply_text(f"⚠️ შეცდომა სიახლეების თარგმნის დროს: {str(e)}")

# ვქმნით პატარა Flask სერვერს Render-ისთვის
app_web = Flask(__name__)

@app_web.route("/")
def home():
    return "Bot is running and active!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app_web.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        print("ERROR: TELEGRAM_TOKEN is missing!")
        
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("analyze", analyze))
    application.add_handler(CommandHandler("news", news))
    
    # ვრთავთ Flask სერვერს ცალკე ნაკადში, რომ Render-მა პორტი დაინახოს
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.start()
    
    print("Bot is running...")
    application.run_polling()
