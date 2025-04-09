import os
import smtplib
import requests
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure Logger
logging.basicConfig(filename="logging.log", format='%(asctime)s %(message)s', filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Load Sensitive Data
API_KEY = os.getenv("OPEN_WEATHER_MAP_API_KEY")
CITIES_LIST = os.getenv("CITIES_LIST")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")

def send_email_report(data: list, recipient: str, sender: str, password: str):
    if not data:
        logger.info("[INFO] No data to email.")
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

        logger.info(f"[INFO] Email sent successfully to {recipient}.")

    except Exception as e:
        logger.error(f"[EMAIL ERROR] {str(e)}")


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
        for city in CITIES_LIST:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

            try:
                response = requests.get(url)
                if response.status_code != 200:
                    logger.error(f"[ERROR] Failed to fetch weather for {city}. Status code: {response.status_code}")
                    logger.error(f"Message: {response.json().get('message')}")
                    continue

                res = response.json()

                if "main" not in res or "weather" not in res or "wind" not in res:
                    logger.error(f"[ERROR] Incomplete data received for {city}: {res}")
                    continue

                data.append(res)

            except Exception as e:
                logger.error(f"[EXCEPTION] Error fetching weather for {city}: {str(e)}")

        return data 
    
    @task
    def transform_data(fetched_data) :
        if fetched_data : 
            transformed_data = []
            for data in fetched_data:

                transformed_data.append({
                        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "city": data["name"],
                        "temperature": data["main"]["temp"],
                        "humidity": data["main"]["humidity"],
                        "weather": data["weather"][0]["description"],
                        "wind_speed": data["wind"]["speed"],
                        "country": data["sys"]["country"],
                })

            return transformed_data 

    @task
    def load_to_csv(data: list):
        if not data:
            logger.info("[INFO] No data to insert.")
            return

        df = pd.DataFrame(data)
        os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
        if os.path.exists(CSV_FILE_PATH):
            df.to_csv(CSV_FILE_PATH, mode='a', header=False, index=False)
        else:
            df.to_csv(CSV_FILE_PATH, index=False)

    @task
    def notify(data: list):
        send_email_report(data, recipient=RECIPIENT_EMAIL, sender=SENDER_EMAIL, password=SENDER_PASSWORD)

    # Task flow
    weather_data = fetch_weather()
    transformed_weather_data = transform_data()
    load_to_csv(transformed_weather_data)
    notify(transformed_weather_data)
