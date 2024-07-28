import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector

# Snowflake connection parameters (replace with your actual credentials)
SNOWFLAKE_ACCOUNT = 'pxeimst-ey17791.snowflakecomputing.com'
SNOWFLAKE_USER = 'amolpatilofficial'
SNOWFLAKE_PASSWORD = 'Shiva@8898'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'DB_DEV_GEOPERFORM'
SNOWFLAKE_SCHEMA = 'PUBLIC'

# Connect to Snowflake
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

conn = init_connection()

# Function to run queries
@st.cache_data
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# Streamlit app
st.title('Snowflake Analytics Dashboard')

# Sidebar for table selection
tables = run_query("SHOW TABLES")
table_names = [table[1] for table in tables]
selected_table = st.sidebar.selectbox('Select a table', table_names)

if selected_table:
    # Get table details
    table_details = run_query(f"DESCRIBE TABLE {selected_table}")
    columns = [col[0] for col in table_details]
    
    # Display table details
    st.header(f"Table: {selected_table}")
    st.dataframe(pd.DataFrame(table_details, columns=['Column', 'Type', 'Kind', 'Null?', 'Default', 'Primary Key', 'Unique Key', 'Check', 'Expression', 'Comment']))

    # Get row count
    row_count = run_query(f"SELECT COUNT(*) FROM {selected_table}")[0][0]
    st.metric("Total Rows", row_count)

    # Sample data
    sample_data = run_query(f"SELECT * FROM {selected_table} LIMIT 10")
    st.subheader("Sample Data")
    st.dataframe(pd.DataFrame(sample_data, columns=columns))

    # Analytics section
    st.header("Analytics")

    # Column distribution
    selected_column = st.selectbox('Select a column for distribution analysis', columns)
    distribution_data = run_query(f"SELECT {selected_column}, COUNT(*) as count FROM {selected_table} GROUP BY {selected_column} ORDER BY count DESC LIMIT 10")
    if distribution_data:
        df_distribution = pd.DataFrame(distribution_data, columns=[selected_column, 'count'])
        fig = px.bar(df_distribution, x=selected_column, y='count', title=f'Top 10 {selected_column} Distribution')
        st.plotly_chart(fig)

    # Correlation matrix
    numeric_columns = [col[0] for col in table_details if col[1] in ('NUMBER', 'FLOAT', 'INTEGER')]
    if len(numeric_columns) > 1:
        st.subheader("Correlation Matrix")
        correlation_query = f"SELECT {', '.join(numeric_columns)} FROM {selected_table} LIMIT 1000"
        correlation_data = run_query(correlation_query)
        df_correlation = pd.DataFrame(correlation_data, columns=numeric_columns)
        correlation_matrix = df_correlation.corr()
        fig = px.imshow(correlation_matrix, title='Correlation Matrix')
        st.plotly_chart(fig)

    # Time series analysis (if date column exists)
    date_columns = [col[0] for col in table_details if 'DATE' in col[1].upper()]
    if date_columns:
        st.subheader("Time Series Analysis")
        selected_date_column = st.selectbox('Select a date column', date_columns)
        selected_metric = st.selectbox('Select a metric', numeric_columns)
        time_series_query = f"""
        SELECT DATE_TRUNC('month', {selected_date_column}) as month, 
               AVG({selected_metric}) as avg_metric
        FROM {selected_table}
        GROUP BY month
        ORDER BY month
        LIMIT 100
        """
        time_series_data = run_query(time_series_query)
        if time_series_data:
            df_time_series = pd.DataFrame(time_series_data, columns=['month', 'avg_metric'])
            fig = px.line(df_time_series, x='month', y='avg_metric', title=f'Average {selected_metric} Over Time')
            st.plotly_chart(fig)

st.sidebar.info("Note: This app uses hardcoded credentials, which is not recommended for production use.")
