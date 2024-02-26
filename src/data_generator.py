import pandas as pd
from faker import Faker
import numpy as np
from datetime import datetime, timedelta
import os


def generate_data():
    # Initialize Faker object
    fake = Faker()
    # Get the current date
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Create the folder path
    folder_path = os.path.join("../data", current_date)

    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    # Generate fake data
    num_records = 1000000  # You can adjust the number of records as per your requirement
    start_date = datetime.now() - timedelta(days=365*5)  # One year ago from now
    end_date = datetime.now()

    # Generate random data for each field
    data = {
        'OrderID': [fake.random_number(digits=6) for _ in range(num_records)],
        'OrderDate': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_records)],
        'Amount': [fake.random.uniform(10.0, 1000.0) for _ in range(num_records)],
        'ProductID': [fake.random_number(digits=4) for _ in range(num_records)],
        'Quantity': [fake.random_number(digits=1) for _ in range(num_records)],
        'CustomerID': [fake.random_number(digits=4) for _ in range(num_records)],
        'Location': [(fake.latitude(), fake.longitude()) for _ in range(num_records)],
        'DeliveryAddress': [fake.address().replace('\n', ', ') for _ in range(num_records)],
        'VendorName': [fake.company() for _ in range(num_records)],
        'PaymentType': [fake.random_element(elements=('Credit Card', 'Debit Card', 'Cash')) for _ in
                        range(num_records)],
        'ProductName': [fake.word() for _ in range(num_records)],
        'ProductCategory': [fake.word() for _ in range(num_records)],
        'ProductSubcategory': [fake.word() for _ in range(num_records)],
        'Price': [fake.random.uniform(5.0, 500.0) for _ in range(num_records)],
        'CustomerFirstName': [fake.first_name() for _ in range(num_records)],
        'CustomerLastName': [fake.last_name() for _ in range(num_records)],
        'Gender': [fake.random_element(elements=('Male', 'Female')) for _ in range(num_records)],
        'DateOfBirth': [fake.date_of_birth(minimum_age=18, maximum_age=90) for _ in range(num_records)],
        'SubscriptionStartDate': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_records)]
    }
    print("created")
    # Create DataFrame
    df = pd.DataFrame(data)

    # File path for saving the Parquet file
    file_path = os.path.join(folder_path, "data.parquet")

    # Save DataFrame to Parquet file
    df.to_parquet(file_path, index=False)

    print("Data saved successfully to data.parquet")
