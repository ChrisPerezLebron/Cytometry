import mysql.connector 
from mysql.connector import errorcode
import os
import csv

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
        CREATE TABLE IF NOT EXISTS Project (
            project_id VARCHAR(50) PRIMARY KEY
        ) ENGINE=InnoDB
        """,
        """
        CREATE TABLE IF NOT EXISTS DiseaseCondition (  
            condition_name VARCHAR(50) PRIMARY KEY
        ) ENGINE=InnoDB
        """,
        """
        CREATE TABLE IF NOT EXISTS Treatment (
            treatment_id VARCHAR(50) PRIMARY KEY
        ) ENGINE=InnoDB
        """,
        """
        CREATE TABLE IF NOT EXISTS Subject (
            project_id VARCHAR(50) NOT NULL,
            subject_id VARCHAR(50) NOT NULL,
            condition_name VARCHAR(50) NOT NULL,
            treatment_id VARCHAR(50),
            age INT NOT NULL,
            sex ENUM('M', 'F') NOT NULL,
            PRIMARY KEY (project_id, subject_id),
            FOREIGN KEY (project_id) REFERENCES Project(project_id),
            FOREIGN KEY (condition_name) REFERENCES DiseaseCondition(condition_name),  
            FOREIGN KEY (treatment_id) REFERENCES Treatment(treatment_id)
        ) ENGINE=InnoDB
        """,
        
        """
        CREATE TABLE IF NOT EXISTS Sample (
            sample_id VARCHAR(50) PRIMARY KEY,
            project_id VARCHAR(50) NOT NULL,
            subject_id VARCHAR(50) NOT NULL,
            sample_type VARCHAR(50) NOT NULL,
            time_from_treatment_start INT,
            response ENUM('y', 'n'),
            FOREIGN KEY (project_id, subject_id) 
                REFERENCES Subject(project_id, subject_id)
        ) ENGINE=InnoDB
        """,
        
        """
        CREATE TABLE IF NOT EXISTS CellCount (
            sample_id VARCHAR(50) PRIMARY KEY,
            b_cell INT NOT NULL,
            cd8_t_cell INT NOT NULL,
            cd4_t_cell INT NOT NULL,
            nk_cell INT NOT NULL,
            monocyte INT NOT NULL,
            FOREIGN KEY (sample_id) REFERENCES Sample(sample_id),
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
    
    # Extract unique reference entities
    projects = {row['project'] for row in rows}
    conditions = {row['condition'] for row in rows}
    treatments = {row['treatment'] for row in rows if row['treatment'] != 'none'}
    
    # Insert reference data
    cursor.executemany(
        "INSERT IGNORE INTO Project (project_id) VALUES (%s)",
        [(p,) for p in projects]
    )
    cursor.executemany(
        "INSERT IGNORE INTO DiseaseCondition (condition_name) VALUES (%s)",
        [(c,) for c in conditions]
    )
    cursor.executemany(
        "INSERT IGNORE INTO Treatment (treatment_id) VALUES (%s)",
        [(t,) for t in treatments]
    )
    
    # Track inserted subjects to avoid duplicates
    inserted_subjects = set()
    
    for row in rows:
        # Prepare subject key
        subj_key = (row['project'], row['subject'])
        
        # Insert subject if new
        if subj_key not in inserted_subjects:
            cursor.execute(
                """
                INSERT INTO Subject (
                    project_id, subject_id, condition_name, 
                    treatment_id, age, sex
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    condition_name = VALUES(condition_name),
                    treatment_id = VALUES(treatment_id),
                    age = VALUES(age),
                    sex = VALUES(sex)
                """,
                (
                    row['project'],
                    row['subject'],
                    row['condition'],
                    row['treatment'] if row['treatment'] != 'none' else None,
                    int(row['age']),
                    row['sex']
                )
            )
            inserted_subjects.add(subj_key)
        
        # Handle nullable fields
        time_val = int(row['time_from_treatment_start']) if row['time_from_treatment_start'] else None
        response_val = row['response'] if row['response'] else None
        
        # Insert sample
        cursor.execute(
            """
            INSERT INTO Sample (
                sample_id, project_id, subject_id, 
                sample_type, time_from_treatment_start, response
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row['sample'],
                row['project'],
                row['subject'],
                row['sample_type'],
                time_val,
                response_val
            )
        )
        
        # Insert cell counts
        cursor.execute(
            """
            INSERT INTO CellCount (
                sample_id, b_cell, cd8_t_cell, 
                cd4_t_cell, nk_cell, monocyte
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row['sample'],
                int(row['b_cell']),
                int(row['cd8_t_cell']),
                int(row['cd4_t_cell']),
                int(row['nk_cell']),
                int(row['monocyte'])
            )
        )
if __name__ == "__main__":
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

