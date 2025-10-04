import os
import re
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# ---------- TRANSFORM HELPERS ----------

def clean_column_names(df):
    """Clean and standardize column names"""
    df.columns = [re.sub(r'[^a-zA-Z0-9_]', '', col.lower().replace(' ', '_')) for col in df.columns]
    return df

def handle_missing_values(df):
    """Handle missing values appropriately based on data type"""
    for column in df.columns:
        if df[column].dtype in ['int64', 'float64']:
            df[column] = df[column].fillna(0)
        elif df[column].dtype == 'object':
            df[column] = df[column].fillna('Unknown')
    return df

def standardize_date_columns(df):
    """Convert potential date columns to proper datetime format"""
    date_patterns = ['date', 'time', 'created', 'updated']
    for column in df.columns:
        if any(pattern in column.lower() for pattern in date_patterns):
            try:
                df[column] = pd.to_datetime(df[column], errors='coerce')
            except Exception:
                print(f"Could not convert {column} to datetime")
    return df

def remove_duplicates(df):
    """Remove duplicate rows"""
    initial_count = len(df)
    df = df.drop_duplicates()
    removed = initial_count - len(df)
    if removed > 0:
        print(f" Removed {removed} duplicate rows")
    return df

def transform_data(df, collection_name):
    """Apply transformations based on collection type"""
    df['_import_timestamp'] = datetime.now()
    df['_source_file'] = collection_name

    if 'order' in collection_name.lower():
        numeric_cols = ['quantity', 'price', 'amount', 'total']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    elif 'product' in collection_name.lower():
        if 'category' in df.columns:
            df['category'] = df['category'].str.strip().str.title()

    elif 'customer' in collection_name.lower():
        if 'name' in df.columns:
            df['name'] = df['name'].str.strip().str.title()
        if 'email' in df.columns:
            df['email'] = df['email'].str.lower().str.strip()

    return df

def validate_data(df, collection_name):
    """Basic validation before loading"""
    print(f"\n--- Validation for {collection_name} ---")
    print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
    print("Missing values per column:\n", df.isnull().sum())
    return len(df) > 0

# ---------- MAIN ETL PIPELINE ----------

def import_excel_files(folder_path, db_name="clearvue_2"):
    """ETL process: Extract → Transform → Load"""
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        processed_files = 0

        for file in os.listdir(folder_path):
            if file.endswith((".xlsx", ".xls")):
                file_path = os.path.join(folder_path, file)
                print(f"\n Processing file: {file}")

                # Extract
                df = pd.read_excel(file_path)

                if not df.empty:
                    collection_name = os.path.splitext(file)[0].lower()

                    # Transform
                    df = clean_column_names(df)
                    df = handle_missing_values(df)
                    df = standardize_date_columns(df)
                    df = remove_duplicates(df)
                    df = transform_data(df, collection_name)

                    # Validate & Load
                    if validate_data(df, collection_name):
                        collection = db[collection_name]
                        data = df.to_dict(orient="records")
                        collection.insert_many(data)
                        print(f" Loaded {len(data)} records into '{collection_name}' collection")
                        processed_files += 1
                    else:
                        print(f" Skipped {collection_name}, failed validation")
                else:
                    print(f" No data in {file}")

        print(f"\n ETL complete: {processed_files} file(s) imported successfully")

    except Exception as e:
        print(" Error in ETL process:", e)

def check_existing_collections(db_name="clearvue_2"):
    """Check collections in MongoDB"""
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collections = db.list_collection_names()
    print("\n Collections in database:")
    for collection in collections:
        count = db[collection].count_documents({})
        print(f" - {collection}: {count} docs")

# ---------- RUN SCRIPT ----------
if __name__ == "__main__":
    folder_path = r"C:\Users\Givenchie\Desktop\NWU 2025\SEMESTER 2\ADV DATABASES-CMPG321\Project\ClearVueBIProj\pipeline_phase_2\pipeline_phase_2_clearvue\exceldata"
    
    check_existing_collections()
    import_excel_files(folder_path)
    check_existing_collections()
