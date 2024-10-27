from flask import Flask, request, jsonify
from ml.model import load_model, plot_last_data_points, train_model
from tools.mongodb.mongo import fetch_all_data
from tools.kafka.topic import create_kafka_topic
from tools.kafka.producer import publish_messages
import numpy as np
import csv
from sklearn.preprocessing import LabelEncoder
import time
import threading

app = Flask(__name__)

kafka_topic_name = "weather_data"
kafka_partitions = 3
kafka_replication = 1
kafka_server = "localhost:9092"
create_kafka_topic(kafka_topic_name, kafka_partitions, kafka_replication, kafka_server)

@app.route('/test', methods=['GET'])
def test():
    return jsonify({'message': 'Test'})

label_encoder = LabelEncoder()

def save_label_encoder():
    global label_encoder
    data = fetch_all_data()
    if not data:
        print('No data available for prediction.')
        return

    label_encoder = LabelEncoder()
    label_encoder.fit([d['summary'] for d in data])
    print('LabelEncoder has been fitted.')


def process_csv_and_publish(csv_file_path, kafka_topic):
    try:
        with open(csv_file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                publish_messages(kafka_topic, message=row)
        time.sleep(5)
        save_label_encoder()
        print("Kafka publishing completed.")
    except Exception as e:
        print(f"An error occurred: {e}")

@app.route('/initialize', methods=['POST'])
def upload_data():
    csv_file_path = '../weather-prediction/data/weather-data.csv'
    thread = threading.Thread(target=process_csv_and_publish, args=(csv_file_path, kafka_topic_name))
    thread.start()
    return jsonify({'message': 'Data initialization started.'}), 200

@app.route('/predict', methods=['POST'])
def predict():
    input = request.json
    if not input:
        return jsonify({'error': 'No data provided in the request body.'}), 400

    model = load_model()

    temperature = float(input.get('temperature'))
    humidity = float(input.get('humidity'))
    if temperature is None or humidity is None:
        return jsonify({"error": "Please provide both temperature and humidity query parameters."}), 400

    X = np.array([[temperature, humidity]])
    predictions = model.predict(X)

    original_label = label_encoder.inverse_transform(predictions)
    plot = plot_last_data_points(20)

    input['summary'] = original_label[0]
    publish_messages(kafka_topic_name, input)

    return jsonify({'predictions': original_label[0], 'plot': plot})

@app.route('/train', methods=['POST'])
def train():
    train_model()
    return jsonify({'message': 'Training model started.'}), 200

if __name__ == "__main__":
    app.run(debug=True, port=5000)
