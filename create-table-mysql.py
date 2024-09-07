import pymysql
import os

# Configuration for connecting to the RDS or MySQL instance
RDS_HOST = os.getenv('RDS_HOST')
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_PORT = int(os.getenv('RDS_PORT', 3306))  # Default MySQL port

def lambda_handler(event, context):
    try:
        # Establish connection to MySQL server
        connection = pymysql.connect(host=RDS_HOST,
                                     user=RDS_USER,
                                     password=RDS_PASSWORD,
                                     port=RDS_PORT)
        
        with connection.cursor() as cursor:
            # Create database
            cursor.execute("CREATE DATABASE IF NOT EXISTS dev1")
            cursor.execute("USE dev1")

            # Create tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INT,
                    signup_date DATE
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bank_accounts (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT,
                    account_number VARCHAR(20),
                    email VARCHAR(100),
                    address VARCHAR(200),
                    balance DECIMAL(18, 2),
                    debt DECIMAL(18, 2),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)

            # Insert records into users table
            cursor.executemany("""
                INSERT INTO users (name, email, age, signup_date) VALUES (%s, %s, %s, %s)
            """, [
                ('Abhinav', 'abhinav@example.com', 8, '2023-07-15'),
                ('Bob Smith', 'bob.smith@example.com', 35, '2022-11-23'),
                ('Charlie Brown', 'charlie.brown@example.org', 42, '2021-03-10'),
                ('Diana Prince', 'diana.prince@example.net', 31, '2024-01-05'),
                ('Evan Turner', 'evan.turner@example.co', 27, '2023-06-12'),
                ('Fiona Apple', 'fiona.apple@website.com', 29, '2023-02-18'),
                ('George Harris', 'george.harris@example.com', 45, '2022-08-25'),
                ('Hannah Davis', 'hannah.davis@subdomain.example.com', 39, '2021-09-14'),
                ('Ivy Green', 'ivy.green@example.com', 24, '2024-04-20'),
                ('Jack Wilson', 'jack.wilson@example.com', 50, '2023-05-22'),
                ('Kelly Fox', 'kelly.fox@com', 30, '2022-12-31'),
                ('Leo King', 'leo.king@.com', 26, '2023-07-05'),
                ('Mia Lee', 'mia.lee@example', 40, '2024-06-01'),
                ('Nina Martinez', 'nina.martinez@example.org', 33, '2023-03-11'),
                ('Oliver Moore', 'oliver.moore@example.com', 37, '2022-10-30'),
                ('Paula White', 'paula.white@example.net', 32, '2021-07-19'),
                ('Quinn Adams', 'quinn.adams@example.com', 29, '2024-03-01'),
                ('Rachel Brown', 'rachel.brown@example.org', 26, '2022-11-20'),
                ('Samuel Black', 'samuel.black@example.com', 40, '2024-02-15'),
                ('Zoe Clarke', 'zoe.clarke@example.com', 23, '2024-09-01')
            ])

            # Insert records into bank_accounts table
            cursor.executemany("""
                INSERT INTO bank_accounts (user_id, account_number, email, address, balance, debt) VALUES (%s, %s, %s, %s, %s, %s)
            """, [
                (1, 'ACC1234567890', 'alice.johnson@example.com', '123 Elm St', 1500.00, 200.00),
                (2, 'ACC1234567891', 'bob.smith@example.com', '456 Oak St', 3500.00, 100.00),
                (3, 'ACC1234567892', 'charlie.brown@example.org', '789 Pine St', 5000.00, 500.00),
                (4, 'ACC1234567893', 'diana.prince@example.net', '101 Maple St', 2000.00, 300.00),
                (5, 'ACC1234567894', 'evan.turner@example.co', '202 Birch St', 800.00, 50.00),
                (6, 'ACC1234567895', 'fiona.apple@website.com', '303 Cedar St', 2200.00, 150.00),
                (7, 'ACC1234567896', 'george.harris@example.com', '404 Spruce St', 3600.00, 400.00),
                (8, 'ACC1234567897', 'hannah.davis@subdomain.example.com', '505 Willow St', 4200.00, 250.00),
                (9, 'ACC1234567898', 'ivy.green@example.com', '606 Fir St', 1300.00, 80.00),
                (10, 'ACC1234567899', 'jack.wilson@example.com', '707 Pine St', 5000.00, 600.00),
                (11, 'ACC123456789A', 'kelly.fox@com', '808 Oak St', 1000.00, 50.00),
                (12, 'ACC123456789B', 'leo.king@.com', '909 Elm St', 1500.00, 150.00),
                (13, 'ACC123456789C', 'mia.lee@example', '1010 Birch St', 800.00, 100.00),
                (14, 'ACC1234567800', 'nina.martinez@example.org', '1111 Cedar St', 2800.00, 200.00),
                (15, 'ACC1234567801', 'oliver.moore@example.com', '1212 Fir St', 3300.00, 300.00),
                (16, 'ACC1234567802', 'paula.white@example.net', '1313 Maple St', 2700.00, 100.00),
                (17, 'ACC1234567803', 'quinn.adams@example.com', '1414 Spruce St', 1500.00, 200.00),
                (18, 'ACC1234567804', 'rachel.brown@example.org', '1515 Pine St', 2000.00, 250.00),
                (19, 'ACC1234567805', 'samuel.black@example.com', '1616 Willow St', 3500.00, 300.00),
                (20, 'ACC1234567807', 'abhinav@example.com', '1717 Oak St', 500.00, 20.00)
            ])

            # Commit changes
            connection.commit()

        return {
            'statusCode': 200,
            'body': 'Database setup and data insertion complete.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }

    finally:
        connection.close()
