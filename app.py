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
        CREATE TABLE IF NOT EXISTS Subjects (
            subject_id INT PRIMARY KEY, 
            condition_name VARCHAR(50) NOT NULL,
            age INT NOT NULL,
            sex ENUM('M', 'F') NOT NULL
        ) ENGINE=InnoDB
        """,
        """
        CREATE TABLE IF NOT EXISTS Treatments (
            treatment_id INT AUTO_INCREMENT PRIMARY KEY,
            treatment_name VARCHAR(50), 
            time_from_treatment_start INT, 
            response ENUM('Y', 'N'),
            subject_id INT NOT NULL,
            FOREIGN KEY (subject_id) REFERENCES Subjects(subject_id)
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
            subject_id INT NOT NULL,
            FOREIGN KEY (subject_id) REFERENCES Subjects(subject_id),
            CHECK (b_cell >= 0),
            CHECK (cd8_t_cell >= 0),
            CHECK (cd4_t_cell >= 0),
            CHECK (nk_cell >= 0),
            CHECK (monocyte >= 0)
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

    

    
    for row in rows:
        print(f'processing row: {row}')
        #push subject to database
        cursor.execute(
            """
            INSERT IGNORE INTO
            Subjects (
                subject_id,
                condition_name,
                age, 
                sex
            )
            VALUES
            (%s, %s, %s, %s)
            """,
            (
                int(row['subject'].replace("sbj", "")),
                row['condition'], 
                int(row['age']),
                row['sex']
                
            )
        )

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
                monocyte,
                subject_id
            )
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                int(row['subject'].replace("sbj", ""))
            )
        )
      
        #push treatment to database
        cursor.execute(
            """
            INSERT INTO
            Treatments (
                treatment_name,
                time_from_treatment_start, 
                response,
                subject_id
            )
            VALUES
            (%s, %s, %s, %s)
            """,
            (
                row['treatment'] if row['treatment'] != 'none' else None, 
                int(row['time_from_treatment_start']) if row['time_from_treatment_start'] else None,
                row['response'].upper() if row['response'] else None,
                int(row['subject'].replace("sbj", ""))
                
            )
        )
        

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
    cursor.execute("SELECT COUNT(*) FROM Subjects")
    print(f"Subjects loaded: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM Samples")
    print(f"Samples loaded: {cursor.fetchone()[0]}")
    
    db.close()
except mysql.connector.Error as err:
    print(f"Database error: {err}")
    db.rollback()



app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'