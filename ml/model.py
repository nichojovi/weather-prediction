from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder
import numpy as np
import pickle
from tools.mongodb.mongo import fetch_all_data
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import base64
import io
from datetime import datetime

def train_model():
    data = fetch_all_data()
    X = np.array([[float(d['temperature']), float(d['humidity'])] for d in data])

    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform([d['summary'] for d in data])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LogisticRegression()
    model.fit(X_train, y_train)

    with open('../weather-prediction/ml/weather_model.pkl', 'wb') as f:
        pickle.dump(model, f)

def load_model():
    with open('../weather-prediction/ml/weather_model.pkl', 'rb') as f:
        return pickle.load(f)

def plot_last_data_points(n):
    data = fetch_all_data()
    recent_data = data[-n:] if len(data) >= 20 else data

    date = [datetime.strptime(d['formatted_date'], "%Y-%m-%d %H:%M:%S.%f %z") for d in recent_data]
    temperature = [float(d['temperature']) for d in recent_data]
    humidity = [float(d['humidity']) for d in recent_data]

    plt.figure(figsize=(10, 5))
    plt.plot(date, temperature, label='Temperature', marker='o', linestyle='-')
    plt.plot(date, humidity, label='Humidity', marker='x', linestyle='--')
    plt.title("Last 20 Data Points of Temperature and Humidity")
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.legend()
    plt.grid(True)

    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode('utf-8')

if __name__ == "__main__":
    train_model()