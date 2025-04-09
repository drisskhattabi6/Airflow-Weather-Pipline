import os
import smtplib
import sqlite3
import requests
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPEN_WEATHER_MAP_API_KEY")
CITIES_LIST = os.getenv("CITIES_LIST")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")

def send_email_report(data: list, recipient: str, sender: str, password: str):
    if not data:
        print("[INFO] No data to email.")
        return

    try:
        # Compose email body as HTML table
        html_rows = "".join(
            f"<tr><td>{d['datetime']}</td><td>{d['city']}</td><td>{d['temperature']}°C</td>"
            f"<td>{d['humidity']}%</td><td>{d['weather']}</td><td>{d['wind_speed']} km/h</td><td>{d['country']}</td></tr>"
            for d in data
        )

        html = f"""
        <html>
        <body>
            <h2>🌤️ Weather Report</h2>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>Datetime</th><th>City</th><th>Temperature</th><th>Humidity</th>
                    <th>Weather</th><th>Wind Speed</th><th>Country</th>
                </tr>
                {html_rows}
            </table>
        </body>
        </html>
        """

        # Set up email
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Weather Data Report"
        msg["From"] = sender
        msg["To"] = recipient

        msg.attach(MIMEText(html, "html"))

        # Send email (Gmail example with TLS)
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())

        print(f"[INFO] Email sent successfully to {recipient}.")

    except Exception as e:
        print(f"[EMAIL ERROR] {str(e)}")

# DB credentials
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'admin',
    'password': '',
    'database': 'weather_db'
}

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='weather_collector',
    schedule_interval='0 */8 * * *',  # every 8 hours
    default_args=default_args,
    description='Fetch weather and store to CSV',
    tags=['weather'],
) as dag:

    @task
    def fetch_weather():
        data = []
        for city in CITYS:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

            try:
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"[ERROR] Failed to fetch weather for {city}. Status code: {response.status_code}")
                    print(f"Message: {response.json().get('message')}")
                    continue

                res = response.json()

                if "main" not in res or "weather" not in res or "wind" not in res:
                    print(f"[ERROR] Incomplete data received for {city}: {res}")
                    continue

                data.append({
                    "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "city": city,
                    "temperature": res["main"]["temp"],
                    "humidity": res["main"]["humidity"],
                    "weather": res["weather"][0]["description"],
                    "wind_speed": res["wind"]["speed"],
                    "country": res["sys"]["country"],
                })

            except Exception as e:
                print(f"[EXCEPTION] Error fetching weather for {city}: {str(e)}")

        return data 

    @task
    def insert_to_csv(data: list):
        if not data:
            print("[INFO] No data to insert.")
            return

        df = pd.DataFrame(data)
        os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
        if os.path.exists(CSV_FILE_PATH):
            df.to_csv(CSV_FILE_PATH, mode='a', header=False, index=False)
        else:
            df.to_csv(CSV_FILE_PATH, index=False)

    @task
    def notify(data: list):
        send_email_report(data, 
                        recipient="drisskhattabi66@gmail.com", 
                        sender="drisskhattabi6@gmail.com", 
                        password="mgwy chst ymzz oypi")

    # Task flow
    weather_data = fetch_weather()
    insert_to_csv(weather_data)
    notify(weather_data)
