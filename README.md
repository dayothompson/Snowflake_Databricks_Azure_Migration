# Snowflake to Azure Data Lake Storage Migration using Databricks Unity Catalog

This project describes the step-by-step guide to migrates data from Snowflake to Azure Data Lake Storage (ADLS) Gen2 using Databricks.

## Tools Used
- **Snowflake**: Source data warehouse
- **Databricks**: Data processing and migration
- **Azure Data Lake Storage Gen2**: Target storage

## Prerequisites
- Azure account with an active subscription and resource group.
- Ensure you have Global Administrator privileges for your Azure account.
- Ensure your Azure account has Contributor or Owner permissions on the subscription and resource group.

## Setup Guide

### 1. Configure Snowflake
1. Sign Up: Visit Snowflake’s trial page, select Azure as your cloud provider, and fill in the details.
2. Verify Email: Confirm your email and set a password to activate the account.
3. Log In: Access Snowflake’s web interface with your credentials.
4. Initial Setup: Create a virtual warehouse (compute resource) and a database/schema for data storage.

    #### Create a virtual warehouse
    1. `CREATE WAREHOUSE IF NOT EXISTS SALES_WH`

    #### Use the warehouse
    2. `USE WAREHOUSE SALES_WH`
    
    #### Create a database
    3. `CREATE DATABASE HOUSE_PRICES`

    #### Use the database
    4. `USE DATABASE HOUSE_PRICES`

    #### Create a schema
    5. `CREATE SCHEMA SALES`

    #### Use the schema
    6. `USE SCHEMA SALES`

    #### Create the tables
    7. `CREATE TABLE IF NOT EXISTS CALGARY (price INT, address STRING, postal_code STRING, bed INT, full_bath INT, half_bath INT, property_area INT, property_type STRING)`
    8. `CREATE TABLE IF NOT EXISTS VANCOUVER (price INT, address STRING, postal_code STRING, bed INT, full_bath INT, half_bath INT, property_area INT, property_type STRING)`

5. Load Data: Upload data from local devices into the 2 new tables. 


### 2. Azure Databricks & ADLS Gen2 Setup
1. Navigate to the Azure Portal, in the search bar, type Databricks and select Azure Databricks.
2. Create a New Databricks Workspace
    1. Click + Create to start a new workspace deployment.
    2. Fill in the required details:

        - **Subscription**: Select your Azure subscription.
        - **Resource Group**: Create a new resource group or select an existing one.
        - **Workspace Name**: Choose a unique name.
        - **Region**: Select an Azure region.
        - **Pricing Tier**: Choose Premium or Enterprise (both support Unity Catalog; Premium is fine for this scenario).

    3. Click Review + Create, then click Create to deploy the workspace.

3. Create an ADLS Gen2 Account
	1.	In the Azure Portal, search for Storage Accounts, Click + Create.
	2.	Select the following:

        - **Subscription**: Choose your Azure subscription.
        - **Resource Group**: Use the same resource group as Databricks.
        - **Storage Account Name**: Provide a globally unique name.
        - **Region**: Choose the same region as your Databricks workspace.
        - **Performance**: Select Standard (or Premium for low-latency workloads).
        - **Redundancy**: Choose an option (LRS, ZRS, GRS, etc.).

	3.	Under Advanced settings, enable Hierarchical Namespace (this is required for ADLS Gen2).
	4.	Click Review + Create, then Create.

4. Create a Container in the ADLS Gen2 Storage Account
	1.	Go to the Storage Account you just created.
	2.	Click Containers under the Data Lake Storage section.
	3.	Click + Container.
	4.	Provide a name (e.g., databricks-data).
	5.	Click Create.


### 3. Azure Connector for Azure Databricks
1. Create an Access Connector for Azure Databricks
	1.	In the Azure Portal, search for Access Connector for Azure Databricks.
	2.	Click + Create.
	3.	Fill in the required details:

        - **Subscription**: Select the same subscription as your Databricks workspace.
        - **Resource Group**: Select the same resource group.
        - **Region**: Choose the same region as the Databricks workspace.
        - **Access Connector Name**: Provide a unique name.

	4.	Click Review + Create, then Create.

2. Grant Access to the Access Connector
	1.	Go to your Storage Account (ADLS Gen2).
	2.	Click Access Control (IAM).
	3.	Click + Add role assignment.
	4.	Assign the following role to the Access Connector:
        • Storage Blob Data Contributor (allows Databricks to read/write).
	5.	Under Assign access to, select Managed Identity.
	6.	Search for the name of your Access Connector and select it.
	7.	Click Save.


### 4. Enable Unity Catalog for the Databricks Workspace
1. Search for the newly created Azure Databricks workspace and click on Launch worspace.
2. Once logged in, click on your workspace dropdown and select Manage Account. (If you are a Global Administrator, it should open fine. If you are not, get someone with Global Administrator privileges to handle step 3). You can also login with https://accounts.azuredatabricks.net/login.
3. Click on User management, locate your user account and under Roles, check Account Admin.
4. Create Metastore: Still in the Manage account portal, select on Catalog and click on Create metastore
5. Fill in the required details:

    - **Name**: Provide a name for your metastore.
    - **Region**: Select the same region as the Databricks workspace.
    - **ADLS Gen2 Path**: Optional.
    - **Access Connector ID**: Optional.

6. Assign the metastore to your Azure Databricks workspace.


### 5. Unity Catalog: Storage Credentials & External Location
1. Create another Access Connector for Azure Databricks: Follow the steps in [Section 3: Azure Connector for Azure Databricks](#3-azure-connector-for-azure-databricks).

2. Create a Storage Credential:
    1. Navigate to Catalog > External Data > Credentials.
    2. Click on Create Credential.
    3. Ensure Storage Credential is selected and for Credential Type, select Azure Managed Identity.
    4. Fill in the required details:
        - **Credential name**: Provide a name.
        - **Access connector ID**: Provide the Access Connector’s Client ID (found in Azure Portal under the connector’s Overview).
        - **Region**: Choose the same region as the Databricks workspace.
        - **Access Connector Name**: Provide a unique name.
    5. Provide the necessary permissions on the Storage credential.

3. Create an External Location:
    1. Navigate to Catalog > External Data > External Locations.
    2. Click on Create external location.
    3. Fill in the required details:
        - **External location name**: Provide a name.
        - **URL**: Enter the ADLS Gen2 path `abfss://<container>@<storage-account>.dfs.core.windows.net/<path>`
        - **Storage credential**: Select the newly created Storage Credential from step 2.
    4. Provide the necessary permissions on the External Location.


### 6. Unity Catalog: Access Snowflake data with Lakehouse Federation
1. Ensure you have the right permissions to create a connection and create a foreign catalog.
2. Create a Snowflake connection: Click on Catalog > External Data > Connections. Click on Create connection.
    1. Fill in the required details:

        **Connection basics:**
        - **Connection name:** Provide a name, e.g., `snowflake-connection`.
        - **Connection type:** Search for Snowflake and select it.
        - **Auth type:** Select Username and password.
        - **Access Connector Name:** Provide a unique name.

        **Authentication:**
        - **Host:** Provide the host name from your Snowflake account, remember to remove the prefix (i.e., no 'jdbc://' or 'https://').
        - **Port:** 443.
        - **User:** Provide your Snowflake username.
        - **Password:** Provide your Snowflake password.

        **Connection details:**
        - **Snowflake warehouse:** Provide the name of your warehouse (e.g., `SALES_WH`).

3. Create foreign catalog: Execute the query below in SQL

    #### Create a foreign catalog for the Snowflake data source
    ```sql
    CREATE FOREIGN CATALOG snowflake_catalog USING CONNECTION `snowflake-connection` OPTIONS (database 'HOUSE_PRICES');
    ```

4. You should be able to see the tables in the Snowflake database from Databricks.


### 7. Data Migration from Snowflake to Azure Data Lake Storage (ADLS) using Databricks
1. By now, all the necessary permissions have been provisioned and you have access to the Snowflake tables and ADLS account.
2. Use the [Snowflake-to-ADLS_Migration.ipynb](./Databricks/Snowflake-to-ADLS_Migration.ipynb) file to complete the steps below.
3. **Read Data from Snowflake:** The [Snowflake-to-ADLS_Migration.ipynb](./Databricks/Snowflake-to-ADLS_Migration.ipynb) script queries the Snowflake tables and loads the data into a Spark DataFrame.
4. **Data Transformation:** It casts various columns into specific data types (e.g., integers and strings) and adds new columns.
5. **Copy data:** Finally, the transformed data is written to ADLS.


