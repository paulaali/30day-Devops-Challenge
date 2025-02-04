import os
import json
import boto3
import requests
import csv
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WeatherDashboard:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        self.s3_client = boto3.client('s3')

    def create_bucket_if_not_exists(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} exists")
        except:
            print(f"Creating bucket {self.bucket_name}")
            try:
                # Simpler creation for us-east-1
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                print(f"Successfully created bucket {self.bucket_name}")
            except Exception as e:
                print(f"Error creating bucket: {e}")

    def fetch_weather(self, city):
        """Fetch weather data from OpenWeather API"""
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "imperial"
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None

    def save_to_s3_csv(self, weather_data_list):
        """Save weather data to S3 bucket in CSV format"""
        if not weather_data_list:
            return False

        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        file_name = f"weather-data/all-cities-{timestamp}.csv"

        try:
            # Prepare CSV data
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer)
            
            # Write CSV headers once
            writer.writerow(['city', 'temperature', 'feels_like', 'humidity', 'description', 'timestamp'])
            
            # Write all weather data rows at once
            for weather_data in weather_data_list:
                writer.writerow([
                    weather_data['city'],
                    weather_data['temperature'],
                    weather_data['feels_like'],
                    weather_data['humidity'],
                    weather_data['description'],
                    weather_data['timestamp']
                ])
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            print(f"Successfully saved weather data for all cities to S3 in CSV format")
            return True
        except Exception as e:
            print(f"Error saving to S3: {e}")
            return False

def main():
    dashboard = WeatherDashboard()
    
    # Create bucket if needed
    dashboard.create_bucket_if_not_exists()
    
    cities = ["Accra", "Yaounde", "Douala", "Kumasi", "Philadelphia", "Seattle", "New York"]
    weather_data_list = []  # List to collect weather data for all cities

    for city in cities:
        print(f"\nFetching weather for {city}...")
        weather_data = dashboard.fetch_weather(city)
        if weather_data:
            weather_entry = {
                'city': city,
                'temperature': weather_data['main']['temp'],
                'feels_like': weather_data['main']['feels_like'],
                'humidity': weather_data['main']['humidity'],
                'description': weather_data['weather'][0]['description'],
                'timestamp': datetime.now().strftime('%Y%m%d-%H%M%S')
            }
            weather_data_list.append(weather_entry)  # Add entry to list
            
            print(f"Temperature: {weather_entry['temperature']}°F")
            print(f"Feels like: {weather_entry['feels_like']}°F")
            print(f"Humidity: {weather_entry['humidity']}%")
            print(f"Conditions: {weather_entry['description']}")
        else:
            print(f"Failed to fetch weather data for {city}")

    # Save all data in CSV format at once
    dashboard.save_to_s3_csv(weather_data_list)

if __name__ == "__main__":
    main()
