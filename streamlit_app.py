import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, OperationalError

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = 'fx57599.central-india.azure'
SNOWFLAKE_USER = 'amolpatilofficial'
SNOWFLAKE_PASSWORD = 'Shiva@8898'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'DB_DEV_GEOPERFORM'

# Available schemas
SCHEMAS = ['STAGING', 'CURATED', 'PRESENTATION']

# Initialize session state
if 'connection_verified' not in st.session_state:
    st.session_state.connection_verified = False
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = SCHEMAS[0]

# Connect to Snowflake
@st.cache_resource
def init_connection():
    try:
        return snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE
        )
    except (ProgrammingError, OperationalError) as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

# Function to run queries
def run_query(query):
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
        except (ProgrammingError, OperationalError) as e:
            st.error(f"Failed to execute query: {str(e)}")
            return None
    return None

# Function to verify connection
def verify_connection():
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_version()")
                version = cur.fetchone()[0]
            st.success(f"Connection successful! Snowflake version: {version}")
            st.session_state.connection_verified = True
        except (ProgrammingError, OperationalError) as e:
            st.error(f"Connection failed: {str(e)}")
            st.session_state.connection_verified = False
    else:
        st.error("Failed to establish connection to Snowflake.")
        st.session_state.connection_verified = False

# Function to get table details
def get_table_details(schema, table):
    details = {}
    
    # Get table structure
    table_structure = run_query(f"DESCRIBE TABLE {schema}.{table}")
    if table_structure:
        details['structure'] = pd.DataFrame(table_structure)
    
    # Get row count
    row_count = run_query(f"SELECT COUNT(*) FROM {schema}.{table}")
    if row_count:
        details['row_count'] = row_count[0][0]
    
    # Get sample data
    sample_data = run_query(f"SELECT * FROM {schema}.{table} LIMIT 5")
    if sample_data:
        columns = [col[0] for col in table_structure]
        details['sample_data'] = pd.DataFrame(sample_data, columns=columns)
    
    return details

# Streamlit app
st.title('Snowflake Analytics Dashboard')

# Verify connection button
if st.button('Verify Snowflake Connection'):
    verify_connection()

# Schema selection
st.session_state.selected_schema = st.selectbox('Select Schema', SCHEMAS)

# Main content
if st.session_state.connection_verified:
    st.header(f'Data Overview - {st.session_state.selected_schema} Schema')
    
    # Get list of tables
    tables = run_query(f"SHOW TABLES IN SCHEMA {st.session_state.selected_schema}")
    if tables:
        # Create a grid layout
        cols = st.columns(3)
        for i, table in enumerate(tables):
            table_name = table[1]
            with cols[i % 3]:
                st.subheader(table_name)
                details = get_table_details(st.session_state.selected_schema, table_name)
                
                if 'row_count' in details:
                    st.metric("Rows", f"{details['row_count']:,}")
                
                if 'structure' in details:
                    with st.expander("Table Structure"):
                        st.dataframe(details['structure'], height=150)
                
                if 'sample_data' in details:
                    with st.expander("Sample Data"):
                        st.dataframe(details['sample_data'], height=150)
                
                # Quick visualizations
                if 'structure' in details and 'sample_data' in details:
                    numeric_columns = details['structure'][details['structure'].iloc[:, 1].str.contains('INT|FLOAT|NUMBER', case=False)].iloc[:, 0].tolist()
                    if numeric_columns:
                        selected_column = st.selectbox(f"Select a column to visualize for {table_name}", numeric_columns)
                        fig = px.histogram(details['sample_data'], x=selected_column, title=f"Distribution of {selected_column}")
                        st.plotly_chart(fig, use_container_width=True)
    else:
        st.error(f"Failed to retrieve table list for {st.session_state.selected_schema} schema. Please check your connection and permissions.")
else:
    st.warning("Please verify your Snowflake connection before proceeding.")

st.sidebar.info("Note: This app uses hardcoded credentials, which is not recommended for production use.")
