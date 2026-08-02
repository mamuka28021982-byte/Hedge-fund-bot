import os
import time
import json
import datetime
import threading
import telebot
import yfinance as yf
import pandas as pd
import numpy as np

# ==========================================
# ⚙️ ტოკენი და კონფიგურაცია
# ==========================================
BOT_TOKEN = "8863157988:AAE2NKxn0i0sSq-nbqvQ9grgQqESSsKagEc"
bot = telebot.TeleBot(BOT_TOKEN)

DB_FILE = "subscribers.json"

SECTORS = {
    "🤖 AI & HIGH-TECH": {
        "NVDA": "Nvidia Corp",
        "MSFT": "Microsoft Corp",
        "PLTR": "Palantir Technologies",
        "SOUN": "SoundHound AI"
    },
    "⚛️ QUANTUM COMPUTING": {
        "IBM": "IBM (Quantum Division)",
        "IONQ": "IonQ Inc",
        "RGTI": "Rigetti Computing"
    },
    "🚀 ROCKETS, SPACE & DEFENSE": {
        "RKLB": "Rocket Lab USA",
        "LMT": "Lockheed Martin",
        "NOC": "Northrop Grumman",
        "KTOS": "Kratos Defense"
    }
}

# ==========================================
# 💾 მონაცემთა ბაზის მართვა (SUBSCRIBERS)
# ==========================================
def load_subscribers():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_subscribers(subs):
    with open(DB_FILE, "w") as f:
        json.dump(list(subs), f)

SUBSCRIBERS = load_subscribers()

# ==========================================
# 📊 ფინანსური და ინდიკატორების გამოთვლა
# ==========================================
def calculate_rsi(data, window=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(data):
    exp1 = data['Close'].ewm(span=12, adjust=False).mean()
    exp2 = data['Close'].ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

def get_executive_statements_and_news(symbol):
    try:
        ticker = yf.Ticker(symbol)
        news_list = ticker.news
        if not news_list:
            return "📰 ახალი კრიტიკული განცხადება არ დაფიქსირებულა."

        formatted_news = []
        keywords = ['CEO', 'CFO', 'says', 'expects', 'guidance', 'contract', 'earnings', 'forecast', 'tech', 'defense']

        for item in news_list[:2]:
            title = item.get('title', '')
            link = item.get('link', '#')
            publisher = item.get('publisher', 'MarketNews')
            
            is_catalyst = any(kw.lower() in title.lower() for kw in keywords)
            tag = "🎙 *[ტოპ-მენეჯმენტი/კატალიზატორი]*" if is_catalyst else "📰"
            
            formatted_news.append(f"{tag} [{title}]({link}) _({publisher})_")

        return "\n  └ ".join(formatted_news)
    except Exception:
        return "📰 სიახლეების წამოღება დროებით შეფერხდა."

def generate_hedge_fund_report():
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    report = f"🏛 *INSTITUTIONAL HEDGE FUND ANALYTICS*\n"
    report += f"⏱ *დრო:* `{now_str}`\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    buy_signals = 0
    sell_signals = 0

    for sector_name, tickers in SECTORS.items():
        report += f"━━━━ *{sector_name}* ━━━━\n"
        
        for symbol, company_name in tickers.items():
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(period="6mo")

                if df.empty or len(df) < 35:
                    continue

                price = df['Close'].iloc[-1]
                prev_price = df['Close'].iloc[-2]
                pct_change = ((price - prev_price) / prev_price) * 100

                df['SMA20'] = df['Close'].rolling(window=20).mean()
                df['SMA50'] = df['Close'].rolling(window=50).mean()
                df['RSI'] = calculate_rsi(df)
                macd, macd_signal = calculate_macd(df)
                
                volatility = df['Close'].pct_change().std() * np.sqrt(252) * 100

                sma20 = df['SMA20'].iloc[-1]
                sma50 = df['SMA50'].iloc[-1]
                rsi = df['RSI'].iloc[-1]
                curr_macd = macd.iloc[-1]
                curr_macd_sig = macd_signal.iloc[-1]

                if rsi > 68 or (price < sma20 and curr_macd < curr_macd_sig):
                    signal = "🚨 *გაყიდვა / მოგების დაფიქსირება (SELL)*"
                    risk_level = "🔴 მაღალი რისკი"
                    sell_signals += 1
                elif rsi < 40 or (price > sma20 and sma20 > sma50 and curr_macd > curr_macd_sig):
                    signal = "🟢 *ყიდვა / აკუმულირება (BUY)*"
                    risk_level = "🟢 დაბალი / ზომიერი"
                    buy_signals += 1
                else:
                    signal = "🟡 *შენარჩუნება / ნეიტრალური (HOLD)*"
                    risk_level = "🟡 საშუალო"

                statements = get_executive_statements_and_news(symbol)
                change_icon = "📈" if pct_change >= 0 else "📉"

                report += (
                    f"▪️ *{symbol}* ({company_name})\n"
                    f"  ├ ფასი: `${price:,.2f}` ({change_icon} `{pct_change:+.2f}%`)\n"
                    f"  ├ RSI (14): `{rsi:.1f}` | ვოლატილობა: `{volatility:.1f}%`\n"
                    f"  ├ რისკის შეფასება: {risk_level}\n"
                    f"  ├ **სავაჭრო სიგნალი:** {signal}\n"
                    f"  └ {statements}\n\n"
                )

            except Exception as e:
                report += f"⚠️ *{symbol}*: მონაცემების დამუშავების შეცდომა.\n\n"

    report += "🎯 *ჰეჯ-ფონდის სტრატეგიული რეზიუმე:*\n"
    if buy_signals > sell_signals:
        report += "🟢 **BULLISH (RISK-ON):** მაღალტექნოლოგიურ და თავდაცვის სექტორში შეინიშნება ინსტიტუციური შესყიდვების ტრენდი."
    elif sell_signals > buy_signals:
        report += "🚨 **BEARISH (RISK-OFF):** რეკომენდებულია რისკების შემცირება ან პოზიციების ჰეჯირება."
    else:
        report += "⚖️ **NEUTRAL:** ბაზარი კონსოლიდაციის ფაზაშია. დაელოდეთ საკვანძო კატალიზატორებს."

    return report

# ==========================================
# 🤖 ტელეგრამ ბოტის ბრძანებები
# ==========================================
@bot.message_handler(commands=['start', 'help'])
def start_command(message):
    chat_id = message.chat.id
    if chat_id not in SUBSCRIBERS:
        SUBSCRIBERS.add(chat_id)
        save_subscribers(SUBSCRIBERS)
    
    welcome_msg = (
        "🏛 *გამარჯობა! მე ვარ თქვენი ჰეჯ-ფონდის ინსტიტუციური ანალიტიკოსი.*\n\n"
        "თქვენ დარეგისტრირდით ავტომატურ **საათობრივ მონიტორინგზე**:\n"
        "• AI, Quantum Computing, Rocketry & Defense სექტორების ანალიზი\n"
        "• CEO/CFO-ების განცხადებები და ბაზრის მოლოდინები\n"
        "• ტექნიკური ინდიკატორები (RSI, MACD, SMA20/50, Volatility)\n"
        "• ყიდვა/გაყიდვის მკაფიო სიგნალები და რისკების შეფასება\n\n"
        "📌 *ბრძანებები:*\n"
        "• `/analyze` - სრული ანალიტიკის მყისიერად მიღება\n"
        "• `/status` - ბოტის აქტივობის შემოწმება"
    )
    bot.send_message(chat_id, welcome_msg, parse_mode="Markdown", disable_web_page_preview=True)

@bot.message_handler(commands=['analyze'])
def manual_analyze(message):
    bot.reply_to(message, "⏳ მიმდინარეობს ბაზრის მონაცემების, განცხადებებისა და სიგნალების ანალიზი...")
    report = generate_hedge_fund_report()
    bot.send_message(message.chat.id, report, parse_mode="Markdown", disable_web_page_preview=True)

@bot.message_handler(commands=['status'])
def status_command(message):
    bot.reply_to(message, f"✅ ბოტი მუშაობს გამართულად. აქტიური მომხმარებლები: {len(SUBSCRIBERS)}")

# ==========================================
# ⏱️ ავტომატური საათობრივი გაგზავნის ციკლი
# ==========================================
def hourly_broadcaster():
    while True:
        time.sleep(3600)
        if SUBSCRIBERS:
            report = generate_hedge_fund_report()
            for chat_id in list(SUBSCRIBERS):
                try:
                    bot.send_message(chat_id, report, parse_mode="Markdown", disable_web_page_preview=True)
                except Exception as e:
                    print(f"შეცდომა chat_id {chat_id}-ზე გაგზავნისას: {e}")

if __name__ == "__main__":
    print("🚀 ჰეჯ-ფონდის ანალიტიკოსი ბოტი წარმატებით გაეშვა...")
    broadcaster_thread = threading.Thread(target=hourly_broadcaster)
    broadcaster_thread.daemon = True
    broadcaster_thread.start()
    bot.infinity_polling()
