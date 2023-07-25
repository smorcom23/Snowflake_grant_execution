import streamlit as st
import snowflake.connector
import os

SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
#SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
#SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]

def check_role_existence(conn, role_name):
    cursor = conn.cursor()
    try:
        # Check if the role exists with securityadmin role
        cursor.execute(f"USE ROLE securityadmin; SHOW ROLES LIKE '{role_name}'")
        role_exists = len(cursor.fetchall()) > 0
        return role_exists
    finally:
        cursor.close()

def execute_procedure(db_name, role_name):
    # Create a Snowflake connection
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
#        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
 #       schema=SNOWFLAKE_SCHEMA
    )

    # Check if the role exists with securityadmin role
    role_exists = check_role_existence(conn, role_name)

    if not role_exists:
        st.error(f"The role '{role_name}' does not exist.")
        conn.close()
        return

    # Create a Snowflake cursor
    cursor = conn.cursor()

    try:
        # Switch to sysadmin role
        cursor.execute("USE ROLE sysadmin")

        # Call the procedure with provided parameters using array_construct
        cursor.execute(f"CALL ag_admin_db.public.create_lsra_db_crud(array_construct('{db_name}'), array_construct('{role_name}'))")
        st.success(f"Procedure executed successfully with Database: {db_name}, Role: {role_name}")
    except Exception as e:
        st.error(f"Error occurred while executing the procedure: {str(e)}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

def main():
    st.title("Snowflake Procedure Execution")

    # Input fields for database name and role
    db_name = st.text_input("Enter the database name:")
    role_name = st.text_input("Enter the role name:")

    # Execute button
    if st.button("Execute"):
        if not db_name or not role_name:
            st.error("Please enter both the database name and role name.")
        else:
            st.info(f"Executing procedure with Database: {db_name}, Role: {role_name}")
            # Call the function to execute the procedure
            execute_procedure(db_name, role_name)

if __name__ == "__main__":
    main()
