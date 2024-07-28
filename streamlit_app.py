import streamlit as st

# Custom CSS to add some color and styling
def local_css(file_name):
    with open(file_name, "r") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def main():
    st.set_page_config(page_title="Snowflake SQL Cheatsheet", layout="wide")
    
    # Custom CSS
    st.markdown("""
    <style>
    .main {
        background-color: #f0f8ff;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #4682B4;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
        color: white;
    }
    .stTabs [aria-selected="true"] {
        background-color: #191970;
    }
    h1 {
        color: #191970;
    }
    h3 {
        color: #4682B4;
    }
    .stCodeBlock {
        background-color: #E6E6FA;
    }
    </style>
    """, unsafe_allow_html=True)

    st.title("Snowflake SQL Cheatsheet")

    # Horizontal tabs for navigation
    tabs = st.tabs(["Data Definition", "Data Manipulation", "Querying Data", "Functions", "Data Loading"])

    with tabs[0]:
        show_data_definition()
    with tabs[1]:
        show_data_manipulation()
    with tabs[2]:
        show_querying_data()
    with tabs[3]:
        show_functions()
    with tabs[4]:
        show_data_loading()

def show_data_definition():
    st.header("Data Definition Language (DDL)")
    
    st.subheader("Create Database")
    st.code("CREATE DATABASE database_name;")
    
    st.subheader("Create Schema")
    st.code("CREATE SCHEMA schema_name;")
    
    st.subheader("Create Table")
    st.code("""
CREATE TABLE table_name (
    column1 datatype,
    column2 datatype,
    ...
);
    """)

def show_data_manipulation():
    st.header("Data Manipulation Language (DML)")
    
    st.subheader("Insert Data")
    st.code("INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...);")
    
    st.subheader("Update Data")
    st.code("UPDATE table_name SET column1 = value1 WHERE condition;")
    
    st.subheader("Delete Data")
    st.code("DELETE FROM table_name WHERE condition;")

def show_querying_data():
    st.header("Querying Data")
    
    st.subheader("Select Data")
    st.code("SELECT column1, column2, ... FROM table_name WHERE condition;")
    
    st.subheader("Join Tables")
    st.code("""
SELECT *
FROM table1
JOIN table2 ON table1.column = table2.column;
    """)
    
    st.subheader("Group By")
    st.code("""
SELECT column1, COUNT(*)
FROM table_name
GROUP BY column1;
    """)

def show_functions():
    st.header("Snowflake Functions")
    
    st.subheader("Date Functions")
    st.code("""
-- Current date
SELECT CURRENT_DATE();

-- Date part
SELECT DATE_PART('month', date_column) FROM table_name;

-- Date trunc
SELECT DATE_TRUNC('month', date_column) FROM table_name;
    """)
    
    st.subheader("String Functions")
    st.code("""
-- Concatenate
SELECT CONCAT(first_name, ' ', last_name) FROM users;

-- Substring
SELECT SUBSTRING(column_name, 1, 3) FROM table_name;
    """)

def show_data_loading():
    st.header("Data Loading")
    
    st.subheader("Copy Command")
    st.code("""
COPY INTO table_name
FROM @stage_name/path/to/file.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    """)
    
    st.subheader("Create External Stage")
    st.code("""
CREATE STAGE my_ext_stage
    URL='s3://my_bucket/path/'
    CREDENTIALS=(AWS_KEY_ID='xxxx' AWS_SECRET_KEY='xxxx');
    """)

if __name__ == "__main__":
    main()
