import streamlit as st
import snowflake.connector
from cryptography.fernet import Fernet

# Read the encryption key from the secure location
with open(".gitignore/encryption_key.txt", "rb") as file:
    encryption_key = file.read()

# Decrypt the configuration file
with open(".gitignore/encrypted_config.txt", "rb") as file:
    encrypted_data = file.read()

cipher_suite = Fernet(encryption_key)
decrypted_data = cipher_suite.decrypt(encrypted_data)
config_data = decrypted_data.decode()

# Parse the configuration data
snowflake_config = {}
for line in config_data.strip().split("\n"):
    key, value = line.split("=")
    snowflake_config[key.strip()] = value.strip()

def check_role_existence(conn, role_name):
    try:
        # Check if the Snowflake connection is active
        cursor = conn.cursor()
        # Check if the role exists with securityadmin role
        cursor.execute("USE ROLE securityadmin")
        cursor.execute(f"SHOW ROLES LIKE '{role_name}'")
        role_exists = len(cursor.fetchall()) > 0

        if role_exists:
            print(f"Role '{role_name}' exists.")
        else:
            print(f"Role '{role_name}' does not exist.")

        cursor.close()
        return role_exists
    except snowflake.connector.errors.OperationalError as e:
        st.error(f"Error occurred while checking role existence: {str(e)}")
        return False

def execute_procedure(db_name, role_name):
    # Create a Snowflake connection
    conn = snowflake.connector.connect(
        user=snowflake_config["SNOWFLAKE_USER"],
        password=snowflake_config["SNOWFLAKE_PASSWORD"],
        account=snowflake_config["SNOWFLAKE_ACCOUNT"],
        database=snowflake_config["SNOWFLAKE_DATABASE"],
        schema=snowflake_config["SNOWFLAKE_SCHEMA"]
    )

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

