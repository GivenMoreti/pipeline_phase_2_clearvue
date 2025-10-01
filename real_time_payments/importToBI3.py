# Import required libraries
# THIS SCRIPT MUST BE RUN ON POWERBI PYTHON SCRIPT.

import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class ClearVueBIProcessor:
    def __init__(self, mongodb_uri="mongodb://localhost:27017/", db_name="clearvue"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[db_name]
    
    def remove_id_columns(self, df):
        """Remove _id columns to avoid merge conflicts"""
        cols_to_keep = [col for col in df.columns if col != '_id']
        return df[cols_to_keep]
    
    def calculate_financial_period(self, date):
        """
        Calculate ClearVue's financial period based on their unique rules.
        Financial month starts on the last Saturday of the preceding month
        and ends on the last Friday of the current month.
        Returns a string in the format 'YYYY-MM'.
        """
        if pd.isna(date):
            return None
        
        if isinstance(date, str):
            date = pd.to_datetime(date)
        
        current_month = date.replace(day=1)
        prev_month = current_month - relativedelta(months=1)
        
        last_day_prev_month = prev_month.replace(day=1) + relativedelta(months=1, days=-1)
        last_saturday_prev = last_day_prev_month - timedelta(days=((last_day_prev_month.weekday() - 5) % 7))
        
        last_day_current_month = current_month + relativedelta(months=1, days=-1)
        last_friday_current = last_day_current_month - timedelta(days=((last_day_current_month.weekday() - 4) % 7))
        
        if last_saturday_prev <= date <= last_friday_current:
            financial_month = current_month.month
            financial_year = current_month.year
        else:
            if date < last_saturday_prev:
                financial_month = prev_month.month
                financial_year = prev_month.year
            else:
                next_month = current_month + relativedelta(months=1)
                financial_month = next_month.month
                financial_year = next_month.year
        
        return f"{financial_year}-{financial_month:02d}"

    def create_financial_calendar_dimension(self, start_date='2018-01-01', end_date='2025-12-31'):
        """
        Creates a comprehensive date dimension table for Power BI.
        """
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        calendar_df = pd.DataFrame({'Date': dates})
        
        calendar_df['Financial_Period'] = calendar_df['Date'].apply(self.calculate_financial_period)
        calendar_df['Year'] = calendar_df['Date'].dt.year
        calendar_df['Month'] = calendar_df['Date'].dt.month
        calendar_df['Month_Name'] = calendar_df['Date'].dt.month_name()
        calendar_df['Quarter'] = calendar_df['Date'].dt.quarter
        calendar_df['Week_of_Year'] = calendar_df['Date'].dt.isocalendar().week
        calendar_df['Day_of_Week'] = calendar_df['Date'].dt.day_name()
        calendar_df['Is_Weekend'] = calendar_df['Day_of_Week'].isin(['Saturday', 'Sunday'])
        
        return calendar_df

    def load_all_collections(self):
        """Load all MongoDB collections into DataFrames and remove _id columns"""
        collections = {
            'products': self.remove_id_columns(pd.DataFrame(list(self.db["products"].find()))),
            'sales_line': self.remove_id_columns(pd.DataFrame(list(self.db["sales line"].find()))),
            'sales_header': self.remove_id_columns(pd.DataFrame(list(self.db["sales header"].find()))),
            'trans_types': self.remove_id_columns(pd.DataFrame(list(self.db["trans types"].find()))),
            'products_styles': self.remove_id_columns(pd.DataFrame(list(self.db["products styles"].find()))),
            'product_brands': self.remove_id_columns(pd.DataFrame(list(self.db["product brands"].find()))),
            'product_categories': self.remove_id_columns(pd.DataFrame(list(self.db["product categories"].find()))),
            'product_ranges': self.remove_id_columns(pd.DataFrame(list(self.db["product ranges"].find()))),
            'purchases_lines': self.remove_id_columns(pd.DataFrame(list(self.db["purchases lines"].find()))),
            'purchases_headers': self.remove_id_columns(pd.DataFrame(list(self.db["purchases headers"].find()))),
            'suppliers': self.remove_id_columns(pd.DataFrame(list(self.db["suppliers"].find()))),
            'representatives': self.remove_id_columns(pd.DataFrame(list(self.db["representatives"].find()))),
            'customer': self.remove_id_columns(pd.DataFrame(list(self.db["customer"].find()))),
            'customer_categories': self.remove_id_columns(pd.DataFrame(list(self.db["customer categories"].find()))),
            'customer_regions': self.remove_id_columns(pd.DataFrame(list(self.db["customer regions"].find()))),
            'customer_account_parameters': self.remove_id_columns(pd.DataFrame(list(self.db["customer account parameters"].find()))),
            'age_analysis': self.remove_id_columns(pd.DataFrame(list(self.db["age analysis"].find()))),
            'payment_lines': self.remove_id_columns(pd.DataFrame(list(self.db["payment lines"].find()))),
            'payment_header': self.remove_id_columns(pd.DataFrame(list(self.db["payment header"].find())))
        }
        return collections

    def create_sales_fact_table(self, collections):
        """Create comprehensive sales fact table"""
        # First merge sales header and line
        sales_fact = pd.merge(
            collections['sales_header'], 
            collections['sales_line'], 
            on="DOC_NUMBER", 
            how="left"
        )
        
        # Then merge with trans_types
        sales_fact = pd.merge(
            sales_fact, 
            collections['trans_types'], 
            on="TRANSTYPE_CODE", 
            how="left"
        )
        
        # Then merge with products
        sales_fact = pd.merge(
            sales_fact, 
            collections['products'], 
            on="INVENTORY_CODE", 
            how="left"
        )
        
        # Finally merge with products_styles
        sales_fact = pd.merge(
            sales_fact, 
            collections['products_styles'], 
            on="INVENTORY_CODE", 
            how="left"
        )
        
        # Add financial period calculation
        if 'TRANS_DATE' in sales_fact.columns:
            sales_fact['FINANCIAL_PERIOD'] = sales_fact['TRANS_DATE'].apply(self.calculate_financial_period)
        
        return sales_fact

    def create_customer_dimension(self, collections):
        """Create customer dimension table"""
        # Start with customer table
        customer_dim = collections['customer'].copy()
        
        # Merge with categories
        if 'CCAT_CODE' in customer_dim.columns:
            customer_dim = pd.merge(
                customer_dim,
                collections['customer_categories'],
                on="CCAT_CODE",
                how="left"
            )
        
        # Merge with regions
        if 'REGION_CODE' in customer_dim.columns:
            customer_dim = pd.merge(
                customer_dim,
                collections['customer_regions'],
                on="REGION_CODE",
                how="left"
            )
        
        # Merge with account parameters
        customer_dim = pd.merge(
            customer_dim,
            collections['customer_account_parameters'],
            on="CUSTOMER_NUMBER",
            how="left"
        )
        
        return customer_dim

    def create_product_dimension(self, collections):
        """Create comprehensive product dimension"""
        product_dim = collections['products'].copy()
        
        # Merge with categories
        if 'PRODCAT_CODE' in product_dim.columns:
            product_dim = pd.merge(
                product_dim,
                collections['product_categories'],
                on="PRODCAT_CODE",
                how="left"
            )
        
        # Merge with brands
        if 'PRODBRA_CODE' in product_dim.columns:
            product_dim = pd.merge(
                product_dim,
                collections['product_brands'],
                on="PRODBRA_CODE",
                how="left"
            )
        
        # Merge with styles
        product_dim = pd.merge(
            product_dim,
            collections['products_styles'],
            on="INVENTORY_CODE",
            how="left"
        )
        
        return product_dim

    def create_payment_fact_table(self, collections):
        """Create payment fact table"""
        payment_fact = pd.merge(
            collections['payment_lines'],
            collections['payment_header'],
            on=["CUSTOMER_NUMBER", "DEPOSIT_REF"],
            how="left"
        )
        
        # Add financial period calculation
        if 'DEPOSIT_DATE' in payment_fact.columns:
            payment_fact['FINANCIAL_PERIOD'] = payment_fact['DEPOSIT_DATE'].apply(self.calculate_financial_period)
        
        return payment_fact

    def generate_power_bi_datasets(self):
        """Generate all datasets for Power BI"""
        collections = self.load_all_collections()
        
        datasets = {
            'sales_fact': self.create_sales_fact_table(collections),
            'customer_dim': self.create_customer_dimension(collections),
            'product_dim': self.create_product_dimension(collections),
            'payment_fact': self.create_payment_fact_table(collections),
            'suppliers_dim': collections['suppliers'],
            'representatives_dim': collections['representatives'],
            'calendar_dim': self.create_financial_calendar_dimension()
        }
        
        return datasets

# --- MAIN EXECUTION ---
# This part runs when the script is executed in Power BI

try:
    # 1. Create the processor
    processor = ClearVueBIProcessor()

    # 2. Generate all the datasets
    datasets = processor.generate_power_bi_datasets()

    # 3. Assign each dataset to a variable
    # These variables will appear in Power BI's Navigator window for you to select
    sales_fact = datasets['sales_fact']
    customer_dim = datasets['customer_dim']
    product_dim = datasets['product_dim']
    payment_fact = datasets['payment_fact']
    suppliers_dim = datasets['suppliers_dim']
    representatives_dim = datasets['representatives_dim']
    calendar_dim = datasets['calendar_dim']

    # Optional: Print the shape of each dataset to verify in Power BI's output console
    print("Data Extraction Complete!")
    print(f"Sales Fact: {sales_fact.shape} rows, {sales_fact.shape[1]} columns")
    print(f"Customer Dimension: {customer_dim.shape} rows, {customer_dim.shape[1]} columns")
    print(f"Product Dimension: {product_dim.shape} rows, {product_dim.shape[1]} columns")
    print(f"Payment Fact: {payment_fact.shape} rows, {payment_fact.shape[1]} columns")
    print(f"Calendar Dimension: {calendar_dim.shape} rows, {calendar_dim.shape[1]} columns")
    
    # Show column names to help with relationship building
    print("\nKey columns for relationships:")
    print(f"Sales Fact columns: {[col for col in sales_fact.columns if 'CUSTOMER' in col or 'INVENTORY' in col or 'PERIOD' in col]}")
    print(f"Customer Dimension columns: {[col for col in customer_dim.columns if 'CUSTOMER' in col]}")
    print(f"Product Dimension columns: {[col for col in product_dim.columns if 'INVENTORY' in col]}")

except Exception as e:
    print(f"Error occurred: {str(e)}")
    print("Please make sure MongoDB is running and the collections exist.")