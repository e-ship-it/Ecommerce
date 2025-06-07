import csv, random , os, traceback
from datetime import datetime as dt
from faker import Faker
from dotenv import load_dotenv
from pathlib import Path

def generate_email(first_name,last_name):
    #input : first_name (Str): First Name of the User
    #input : last_name (Str): Last Name of the User
    #return : str: generated email id

    random_num = random.randint(1000, 9999)
    return f"{first_name}.{last_name}_{random_num}@example.com"

def generate_user_data(user_id):
    # input: user_id(int) : USER ID for which the data is to be generated
    # return : list of values generated against the id
    fake = Faker()

    name = fake.name()
    email = generate_email(name.split()[0],name.split()[1])
    birthdate = fake.date_of_birth(minimum_age = 18, maximum_age = 100).strftime("%Y-%m-%d")
    registration_date = fake.date_this_decade().strftime("%Y-%m-%d")
    address = fake.address()
    phone_number = fake.phone_number()
    return [user_id, name, email, birthdate, registration_date, address, phone_number]


try: 
    cwd_ = os.path.dirname(os.path.abspath(__file__))
    output_file = cwd_ + "/dataset/bittlingmayer/amazonreviews/users.csv"

    with open(output_file,newline='',mode = 'w') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(["user_id", "name", "email", "birthdate", "registration_date", "address", "phone_number"])

        # Write user data for each user
        for user_id in range(1,246209):
            writer.writerow(generate_user_data(user_id))
    print("Done")
except Exception as e:
    traceback.print_exc()