# Import required libraries
# THIS SCRIPT MUST BE RUN ON POWERBI PYTHON SCRIPT.

import pandas as pd
from pymongo import MongoClient

# Initialize global variables first
sales_header = pd.DataFrame()
sales_line = pd.DataFrame()
products = pd.DataFrame()
customer = pd.DataFrame()
trans_types = pd.DataFrame()
products_styles = pd.DataFrame()
product_brands = pd.DataFrame()
product_categories = pd.DataFrame()
product_ranges = pd.DataFrame()
purchases_headers = pd.DataFrame()
purchases_lines = pd.DataFrame()
suppliers = pd.DataFrame()
customer_categories = pd.DataFrame()
customer_regions = pd.DataFrame()
customer_account_parameters = pd.DataFrame()
age_analysis = pd.DataFrame()
payment_lines = pd.DataFrame()
payment_header = pd.DataFrame()
representatives = pd.DataFrame()

def import_mongodb_collections():
    """Import all MongoDB collections directly into Power BI as separate tables"""
    global sales_header, sales_line, products, customer, trans_types
    global products_styles, product_brands, product_categories, product_ranges
    global purchases_headers, purchases_lines, suppliers, customer_categories
    global customer_regions, customer_account_parameters, age_analysis
    global payment_lines, payment_header, representatives
    
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["clearvue_2"]  # Using your actual database name
        
        print("Connected to MongoDB successfully!")
        
        # List all collections in the database
        collection_names = db.list_collection_names()
        print(f"Available collections: {collection_names}")
        
        # Import each collection as a separate DataFrame
        # These will appear as separate tables in Power BI Navigator
        
        # Sales related collections
        sales_header = pd.DataFrame(list(db["sales_header"].find()))
        sales_line = pd.DataFrame(list(db["sales_line"].find()))
        trans_types = pd.DataFrame(list(db["trans_types"].find()))
        
        # Product related collections  
        products = pd.DataFrame(list(db["products"].find()))
        products_styles = pd.DataFrame(list(db["products_styles"].find()))
        product_brands = pd.DataFrame(list(db["product_brands"].find()))
        product_categories = pd.DataFrame(list(db["product_categories"].find()))
        product_ranges = pd.DataFrame(list(db["product_ranges"].find()))
        
        # Purchase related collections
        purchases_headers = pd.DataFrame(list(db["purchases_headers"].find()))
        purchases_lines = pd.DataFrame(list(db["purchases_lines"].find()))
        suppliers = pd.DataFrame(list(db["suppliers"].find()))
        
        # Customer related collections
        customer = pd.DataFrame(list(db["customer"].find()))
        customer_categories = pd.DataFrame(list(db["customer_categories"].find()))
        customer_regions = pd.DataFrame(list(db["customer_regions"].find()))
        customer_account_parameters = pd.DataFrame(list(db["customer_account_parameters"].find()))
        
        # Financial collections
        age_analysis = pd.DataFrame(list(db["age_analysis"].find()))
        payment_lines = pd.DataFrame(list(db["payment_lines"].find()))
        payment_header = pd.DataFrame(list(db["payment_header"].find()))
        
        # Other collections
        representatives = pd.DataFrame(list(db["representatives"].find()))
        
        # Remove MongoDB _id columns to avoid conflicts
        def remove_id(df):
            if '_id' in df.columns:
                return df.drop('_id', axis=1)
            return df
        
        # Apply to all dataframes
        sales_header = remove_id(sales_header)
        sales_line = remove_id(sales_line)
        trans_types = remove_id(trans_types)
        products = remove_id(products)
        products_styles = remove_id(products_styles)
        product_brands = remove_id(product_brands)
        product_categories = remove_id(product_categories)
        product_ranges = remove_id(product_ranges)
        purchases_headers = remove_id(purchases_headers)
        purchases_lines = remove_id(purchases_lines)
        suppliers = remove_id(suppliers)
        customer = remove_id(customer)
        customer_categories = remove_id(customer_categories)
        customer_regions = remove_id(customer_regions)
        customer_account_parameters = remove_id(customer_account_parameters)
        age_analysis = remove_id(age_analysis)
        payment_lines = remove_id(payment_lines)
        payment_header = remove_id(payment_header)
        representatives = remove_id(representatives)
        
        # Print summary
        print("\n=== DATA IMPORT SUMMARY ===")
        print(f"Sales Header: {sales_header.shape[0]} rows, {sales_header.shape[1]} columns")
        print(f"Sales Line: {sales_line.shape[0]} rows, {sales_line.shape[1]} columns") 
        print(f"Products: {products.shape[0]} rows, {products.shape[1]} columns")
        print(f"Customers: {customer.shape[0]} rows, {customer.shape[1]} columns")
        print(f"Payment Lines: {payment_lines.shape[0]} rows, {payment_lines.shape[1]} columns")
        
        print("\nAll collections imported successfully!")
        print("You can now build relationships in Power BI using:")
        print("  - DOC_NUMBER (between sales_header and sales_line)")
        print("  - INVENTORY_CODE (between sales_line and products)")
        print("  - CUSTOMER_NUMBER (between sales_header and customer)")
        print("  - TRANSTYPE_CODE (between sales_header and trans_types)")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting steps:")
        print("1. Make sure MongoDB is running on localhost:27017")
        print("2. Check if database 'clearvue' exists")
        print("3. Verify collection names match exactly")
        return False

# Execute the import
success = import_mongodb_collections()

if success:
    print("\n✅ Ready to build relationships in Power BI!")
else:
    print("\n❌ Import failed. Check error messages above.")
    print("Empty DataFrames created for all tables.")