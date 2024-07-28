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
        
        # Get total storage used
        storage_usage = run_query(f"SELECT STORAGE_USAGE FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS WHERE TABLE_SCHEMA = '{schema}'")
        if storage_usage:
            total_storage = sum(row[0] for row in storage_usage)
            summary['storage_usage'] = f"{total_storage / (1024 * 1024 * 1024):.2f} GB"
    else:
        summary = None
    
    return summary

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
            col1, col2, col3 = st.columns(3)
            col1.metric("📚 Total Tables", summary['total_tables'])
            col2.metric("🔢 Total Rows", f"{summary['total_rows']:,}")
            col3.metric("💾 Storage Usage", summary['storage_usage'])
            
            # Additional schema details
            st.subheader("📋 Schema Details")
            schema_details = run_query(f"DESCRIBE SCHEMA {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}")
            if schema_details:
                st.table(pd.DataFrame(schema_details, columns=['Property', 'Value']))
            
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
                table_details = run_query(f"DESCRIBE TABLE {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table}")
                if table_details:
                    st.subheader(f"📋 Table: {selected_table}")
                    df_details = pd.DataFrame(table_details)
                    st.dataframe(df_details)
                    
                    # Use the first column as the list of column names
                    columns = [row[0] for row in table_details]
                    
                    # Enhanced Table statistics
                    st.subheader("📊 Table Statistics")
                    col1, col2, col3, col4 = st.columns(4)
                    row_count = run_query(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table}")
                    if row_count:
                        col1.metric("Total Rows", f"{row_count[0][0]:,}")
                    
                    size_query = f"""
                    SELECT TABLE_NAME, ROW_COUNT, BYTES, CREATED, LAST_ALTERED
                    FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{st.session_state.selected_schema}' AND TABLE_NAME = '{selected_table}'
                    """
                    size_info = run_query(size_query)
                    if size_info:
                        col2.metric("Size", f"{size_info[0][2] / (1024*1024*1024):.2f} GB")
                        col3.metric("Avg Row Size", f"{size_info[0][2] / size_info[0][1]:.2f} bytes")
                        col4.metric("# of Columns", len(columns))
                    
                    # Additional table metadata
                    st.subheader("📌 Table Metadata")
                    meta_col1, meta_col2 = st.columns(2)
                    meta_col1.info(f"Created: {size_info[0][3]}")
                    meta_col2.info(f"Last Altered: {size_info[0][4]}")
                    
                    # Owner access information
                    st.subheader("👤 Owner Access Information")
                    owner_query = f"""
                    SELECT PRIVILEGE, GRANTEE, GRANTED_BY
                    FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLE_PRIVILEGES
                    WHERE TABLE_SCHEMA = '{st.session_state.selected_schema}' AND TABLE_NAME = '{selected_table}'
                    """
                    owner_info = run_query(owner_query)
                    if owner_info:
                        df_owner = pd.DataFrame(owner_info, columns=['Privilege', 'Grantee', 'Granted By'])
                        st.dataframe(df_owner)
                    else:
                        st.warning("Unable to retrieve owner access information. You may not have the required permissions.")
                    
                    # Sample data
                    st.subheader("👀 Sample Data")
                    sample_data = run_query(f"SELECT * FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table} LIMIT 10")
                    if sample_data:
                        st.dataframe(pd.DataFrame(sample_data, columns=columns))
                    
                    # Column statistics
                    st.subheader("🔢 Column Statistics")
                    selected_column = st.selectbox('Select a column for detailed statistics', columns)
                    col_stats_query = f"""
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(DISTINCT {selected_column}) as distinct_count,
                        AVG(CAST({selected_column} AS FLOAT)) as avg_value,
                        MIN({selected_column}) as min_value,
                        MAX({selected_column}) as max_value
                    FROM {SNOWFLAKE_DATABASE}.{st.session_state.selected_schema}.{selected_table}
                    """
                    col_stats = run_query(col_stats_query)
                    if col_stats:
                        stats_col1, stats_col2, stats_col3, stats_col4, stats_col5 = st.columns(5)
                        stats_col1.metric("Total Count", f"{col_stats[0][0]:,}")
                        stats_col2.metric("Distinct Count", f"{col_stats[0][1]:,}")
                        stats_col3.metric("Average", f"{col_stats[0][2]:.2f}")
                        stats_col4.metric("Minimum", col_stats[0][3])
                        stats_col5.metric("Maximum", col_stats[0][4])
                    
                    # Analytics section
                    st.header("📈 Analytics")
                    
                    # Column distribution
                    st.subheader("Column Distribution")
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
                    numeric_columns = [col[0] for col in table_details if 'NUMBER' in col[1].upper() or 'INT' in col[1].upper() or 'FLOAT' in col[1].upper()]
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
                    date_columns = [col[0] for col in table_details if 'DATE' in col[1].upper() or 'TIMESTAMP' in col[1].upper()]
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
                else:
                    st.error(f"Failed to retrieve details for table {selected_table}")
        else:
            st.error(f"Failed to retrieve table list for {st.session_state.selected_schema} schema. Please check your connection and permissions.")

else:
    st.warning("⚠️ Please verify your Snowflake connection before proceeding.")

st.sidebar.info("ℹ️ Note: This app uses hardcoded credentials, which is not recommende")
