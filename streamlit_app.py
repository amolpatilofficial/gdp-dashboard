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

# Custom theme
st.set_page_config(layout="wide", page_title="Snowflake Analytics Dashboard")

# Custom CSS
st.markdown("""
<style>
    .reportview-container {
        background: linear-gradient(to right, #e0f2fe, #ffffff);
    }
    .sidebar .sidebar-content {
        background: linear-gradient(to bottom, #1e3a8a, #3b82f6);
    }
    .Widget>label {
        color: #1e3a8a;
        font-weight: bold;
    }
    .stButton>button {
        color: white;
        background-color: #3b82f6;
        border-radius: 5px;
    }
    .stButton>button:hover {
        background-color: #1e3a8a;
    }
</style>
""", unsafe_allow_html=True)

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

# Function to get executive summary
def get_executive_summary(schema):
    summary = {}
    
    # Get list of tables
    tables = run_query(f"SHOW TABLES IN {SNOWFLAKE_DATABASE}.{schema}")
    if tables:
        summary['total_tables'] = len(tables)
        
        # Get total rows across all tables
        total_rows = 0
        for table in tables:
            table_name = table[1]
            row_count = run_query(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{schema}.{table_name}")
            if row_count:
                total_rows += row_count[0][0]
        summary['total_rows'] = total_rows
    else:
        summary = None
    
    return summary

# Function to get detailed table info
def get_table_info(schema, table):
    info = {}
    
    # Get basic table info
    table_info = run_query(f"""
        SELECT TABLE_TYPE, ROW_COUNT, BYTES, RETENTION_TIME, CREATED, LAST_ALTERED
        FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    """)
    if table_info:
        info['type'] = table_info[0][0]
        info['row_count'] = table_info[0][1]
        info['size_bytes'] = table_info[0][2]
        info['retention_time'] = table_info[0][3]
        info['created'] = table_info[0][4]
        info['last_altered'] = table_info[0][5]
    
    # Get column info
    columns = run_query(f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
    """)
    if columns:
        info['columns'] = columns
    
    return info

# Streamlit app
st.title('🔍 Snowflake Analytics Dashboard')

# Sidebar
st.sidebar.title("Navigation")
verify_connection_button = st.sidebar.button('🔗 Verify Snowflake Connection')
if verify_connection_button:
    verify_connection()

# Schema selection
st.session_state.selected_schema = st.sidebar.selectbox('📊 Select Schema', SCHEMAS)

# Main content
if st.session_state.connection_verified:
    page = st.sidebar.radio('📑 Pages', ['Executive Summary', 'Table Analytics'])
    
    if page == 'Executive Summary':
        st.header(f'📈 Executive Summary - {st.session_state.selected_schema} Schema')
        summary = get_executive_summary(st.session_state.selected_schema)
        if summary:
            col1, col2 = st.columns(2)
            col1.metric("📚 Total Tables", summary.get('total_tables', 'N/A'))
            col2.metric("🔢 Total Rows", f"{summary.get('total_rows', 'N/A'):,}")
            
            # Additional schema details
            st.subheader("📋 Schema Details")
            schema_details = run_query(f"DESCRIBE SCHEMA {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}")
            if schema_details:
                st.table(pd.DataFrame(schema_details, columns=['Property', 'Value']))
            
            # Table list with details
            st.subheader("📊 Tables in Schema")
            tables = run_query(f"""
                SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, CREATED, LAST_ALTERED
                FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{st.session_state.selected_schema}'
                ORDER BY BYTES DESC
            """)
            if tables:
                df_tables = pd.DataFrame(tables, columns=['Table Name', 'Type', 'Row Count', 'Size (Bytes)', 'Created', 'Last Altered'])
                df_tables['Size (MB)'] = df_tables['Size (Bytes)'] / (1024 * 1024)
                df_tables['Size (MB)'] = df_tables['Size (MB)'].round(2)
                st.dataframe(df_tables)

                # Visualize table sizes
                fig = px.bar(df_tables, x='Table Name', y='Size (MB)', title='Table Sizes in Schema')
                st.plotly_chart(fig)
            
            # Most recent updates
            st.subheader("🕒 Recent Updates")
            recent_updates = run_query(f"""
                SELECT TABLE_NAME, LAST_ALTERED
                FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{st.session_state.selected_schema}'
                ORDER BY LAST_ALTERED DESC
                LIMIT 5
            """)
            if recent_updates:
                st.table(pd.DataFrame(recent_updates, columns=['Table', 'Last Updated']))
        else:
            st.error(f"Failed to retrieve executive summary for {st.session_state.selected_schema} schema. Please check your connection and try again.")
    
    elif page == 'Table Analytics':
        st.header(f'📊 Table Analytics - {st.session_state.selected_schema} Schema')
        tables = run_query(f"SHOW TABLES IN {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}")
        if tables:
            table_names = [table[1] for table in tables]
            selected_table = st.selectbox('Select a table', table_names)
            
            if selected_table:
                # Table details
                table_info = get_table_info(st.session_state.selected_schema, selected_table)
                if table_info:
                    st.subheader(f"📋 Table: {selected_table}")
                    
                    # Display basic table info
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Table Type", table_info.get('type', 'N/A'))
                    col2.metric("Row Count", f"{table_info.get('row_count', 'N/A'):,}")
                    col3.metric("Size", f"{table_info.get('size_bytes', 0) / (1024*1024):.2f} MB")
                    
                    col1, col2 = st.columns(2)
                    col1.metric("Created", table_info.get('created', 'N/A'))
                    col2.metric("Last Altered", table_info.get('last_altered', 'N/A'))
                    
                    # Display column info
                    st.subheader("Column Details")
                    if 'columns' in table_info:
                        df_columns = pd.DataFrame(table_info['columns'], columns=['Name', 'Type', 'Nullable', 'Max Length', 'Precision', 'Scale'])
                        st.dataframe(df_columns)
                    
                    # Sample data
                    st.subheader("👀 Sample Data")
                    sample_data = run_query(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table} LIMIT 10")
                    if sample_data:
                        st.dataframe(pd.DataFrame(sample_data, columns=[col[0] for col in table_info['columns']]))
                    
                    # Analytics section
                    st.header("📈 Analytics")
                    
                    # Column distribution
                    st.subheader("Column Distribution")
                    columns = [col[0] for col in table_info['columns']]
                    selected_column = st.selectbox('Select a column for distribution analysis', columns)
                    distribution_data = run_query(f"""
                        SELECT {selected_column}, COUNT(*) as count 
                        FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table} 
                        GROUP BY {selected_column} 
                        ORDER BY count DESC 
                        LIMIT 10
                    """)
                    if distribution_data:
                        df_distribution = pd.DataFrame(distribution_data, columns=[selected_column, 'count'])
                        fig = px.bar(df_distribution, x=selected_column, y='count', title=f'Top 10 {selected_column} Distribution')
                        st.plotly_chart(fig)
                    
                    # Correlation matrix
                    numeric_columns = [col[0] for col in table_info['columns'] if col[1] in ('NUMBER', 'FLOAT', 'INT', 'INTEGER', 'BIGINT', 'DECIMAL')]
                    if len(numeric_columns) > 1:
                        st.subheader("Correlation Matrix")
                        correlation_query = f"SELECT {', '.join(numeric_columns)} FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table} LIMIT 1000"
                        correlation_data = run_query(correlation_query)
                        if correlation_data:
                            df_correlation = pd.DataFrame(correlation_data, columns=numeric_columns)
                            correlation_matrix = df_correlation.corr()
                            fig = px.imshow(correlation_matrix, title='Correlation Matrix')
                            st.plotly_chart(fig)
                    
                    # Time series analysis
                    date_columns = [col[0] for col in table_info['columns'] if 'DATE' in col[1].upper() or 'TIMESTAMP' in col[1].upper()]
                    if date_columns and numeric_columns:
                        st.subheader("Time Series Analysis")
                        selected_date_column = st.selectbox('Select a date column', date_columns)
                        selected_metric = st.selectbox('Select a metric', numeric_columns)
                        time_series_query = f"""
                        SELECT DATE_TRUNC('month', {selected_date_column}) as month, 
                               AVG({selected_metric}) as avg_metric
                        FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table}
                        GROUP BY month
                        ORDER BY month
                        LIMIT 100
                        """
                        time_series_data = run_query(time_series_query)
                        if time_series_data:
                            df_time_series = pd.DataFrame(time_series_data, columns=['month', 'avg_metric'])
                            fig = px.line(df_time_series, x='month', y='avg_metric', title=f'Average {selected_metric} Over Time')
                            st.plotly_chart(fig)
                    
                    # Data quality checks
                    st.subheader("Data Quality Checks")
                    
                    # Null value check
                    null_checks = []
                    for col in columns:
                        null_query = f"""
                        SELECT COUNT(*) as null_count
                        FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table}
                        WHERE {col} IS NULL
                        """
                        null_result = run_query(null_query)
                        if null_result:
                            null_checks.append((col, null_result[0][0]))
                    
                    df_null_checks = pd.DataFrame(null_checks, columns=['Column', 'Null Count'])
                    df_null_checks['Null Percentage'] = (df_null_checks['Null Count'] / table_info['row_count']) * 100
                    df_null_checks['Null Percentage'] = df_null_checks['Null Percentage'].round(2)
                    st.dataframe(df_null_checks)
                    
                    # Visualize null percentages
                    fig = px.bar(df_null_checks, x='Column', y='Null Percentage', title='Null Percentage by Column')
                    st.plotly_chart(fig)
                    
                else:
                    st.error(f"Failed to retrieve details for table {selected_table}")
        else:
