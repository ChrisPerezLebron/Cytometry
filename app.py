import mysql.connector 
from mysql.connector import errorcode
import os
import csv
from flask import Flask

def create_database(cursor):
    """Create database if not exists"""
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS clinical_trial_db")
        cursor.execute("USE clinical_trial_db")
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
        exit(1)

#Uses the InnoDB engine 
def create_tables(cursor):
    """Create database tables"""
    tables = [
        """
        CREATE TABLE IF NOT EXISTS Treatments (
            treatment_id INT PRIMARY KEY,
            treatment_name VARCHAR(50) NOT NULL, 
            time_from_treatment_start INT not NULL, 
            response ENUM('Y', 'N')
        ) ENGINE=InnoDB
        """,
        """
        CREATE TABLE IF NOT EXISTS Samples (
            sample_id INT PRIMARY KEY,
            project VARCHAR(50) NOT NULL,
            sample_type VARCHAR(50),
            b_cell INT NOT NULL,
            cd8_t_cell INT NOT NULL, 
            cd4_t_cell INT NOT NULL, 
            nk_cell INT NOT NULL, 
            monocyte INT NOT NULL,
            CHECK (b_cell >= 0),
            CHECK (cd8_t_cell >= 0),
            CHECK (cd4_t_cell >= 0),
            CHECK (nk_cell >= 0),
            CHECK (monocyte >= 0)
        ) ENGINE=InnoDB 
        """, 
        """
        CREATE TABLE IF NOT EXISTS Subjects (
            subject_id INT PRIMARY KEY, 
            condition_name VARCHAR(50) NOT NULL,
            age INT NOT NULL,
            sex ENUM('M', 'F') NOT NULL,
            sample_id INT NOT NULL,
            treatment_id INT NOT NULL,
            FOREIGN KEY (sample_id) REFERENCES Samples(sample_id),  
            FOREIGN KEY (treatment_id) REFERENCES Treatments(treatment_id)
        ) ENGINE=InnoDB
        """
    ]
    
    #execute table creation scripts
    for sql in tables:
        try:
            cursor.execute(sql)
            print(f"Successfully executed: {sql[:50]}...")
        except mysql.connector.Error as err:
            print(f"Error executing SQL: {err}")
            print(f"Problematic SQL: {sql}")
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table already exists - continuing")
            else:
                raise  # Re-raise critical errors


def load_data_from_csv(cursor, file_path):
    """Load CSV data into MySQL database"""
    # Read CSV data
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    
    # Track inserted subjects to avoid duplicates
    inserted_samples = {}
    inserted_treatments = {}  
    inserted_subjects = {}
    

    
    for row in rows:

        if row['sample'] not in inserted_samples: 
            #push sample to database 
            cursor.execute(
                """
                INSERT INTO
                Samples (
                    sample_id,
                    project,
                    sample_type,
                    b_cell,
                    cd8_t_cell,
                    cd4_t_cell,
                    nk_cell,
                    monocyte
                )
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    int(row['sample'].replace("s", "")),
                    row['project'],
                    row['sample_type'],
                    int(row['b_cell']),
                    int(row['cd8_t_cell']),
                    int(row['cd4_t_cell']), 
                    int(row['nk_cell']), 
                    int(row['monocyte']),
                )
            )

            # Add sample to inserted samples 
            inserted_samples.add(row['sample'])
        if row['treatment'] not in treatments: 
            treatments[row['treatment']] = [
                
            ]
            #push treatment to database

        

csv_path = 'cell-count.csv'

if not os.path.exists(csv_path):
    print(f"Error: File not found at {csv_path}")
    exit(1)

try:
    # Establish connection to db using username and password provided in the .env file
    db = mysql.connector.connect(host="localhost", user=os.getenv("DB_USER"), passwd=os.getenv("DB_PASS"))
    # Get cursor
    cursor = db.cursor()
    
    # Create database and tables
    create_database(cursor)
    create_tables(cursor)
    
    # Load CSV data
    load_data_from_csv(cursor, csv_path)
    db.commit()
    
    print("Data loaded successfully!")
    
    # Verify record counts
    cursor.execute("SELECT COUNT(*) FROM Subject")
    print(f"Subjects loaded: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM Sample")
    print(f"Samples loaded: {cursor.fetchone()[0]}")
    
except mysql.connector.Error as err:
    print(f"Database error: {err}")
    db.rollback()



app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'