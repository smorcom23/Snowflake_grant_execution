import streamlit as st
import snowflake.connector
import os

def execute_procedure(db_name, role_name):
    # Read Snowflake connection information from the environment variables
    snowflake_config = {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA")
    }

    # Create a Snowflake connection
    conn = snowflake.connector.connect(
        user=snowflake_config["user"],
        password=snowflake_config["password"],
        account=snowflake_config["account"],
        database=snowflake_config["database"],
        schema=snowflake_config["schema"]
    )

    # Create a Snowflake cursor
    cursor = conn.cursor()

    try:
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
