
# ETL Pipeline Databricks Notebook

This repository contains a Databricks notebook that implements an ETL (Extract, Transform, Load) pipeline for ingesting data from an API and Azure Blob Storage, performing necessary transformations, and storing the results in a Delta table.

## Prerequisites

Before you can run the notebook, you need the following:

1. **Databricks Account**: You must have access to a Databricks workspace.
2. **Azure Resources**:
   - **Azure Storage Account**: For storing raw data files.
   - **Azure Key Vault**: For securely managing secrets (like API keys and service principal credentials).
   - **App Registration**: Used as a Service Principal for accessing Azure resources.

![image](https://github.com/user-attachments/assets/861178b5-32e8-48e9-a398-9f32044f715e)

## Setup Instructions

### 1. Clone the Repository
Clone this repository to your local machine:

\`\`\`bash
git clone [https://github.com/DiMar-dev/spatially-databricks-etl](https://github.com/DiMar-dev/spatially-databricks-etl)
\`\`\`

### 2. Import the Notebook into Databricks

1. Log in to your Databricks workspace.
2. Navigate to the workspace where you want to import the notebook.
3. Righ-click your workspace name and select "Import".
4. Upload the file from the cloned repository.

### 3. Set Up Azure Resources

Ensure that you have the following resources set up in Azure:

- **Azure Storage Account**: Create a storage account and a container to store your CSV files.
- **App Registration**: Register an application in Azure AD, create a service principal, and assign it the necessary roles for accessing the storage account.
- **Azure Key Vault**: Store your secrets, including:
  - \`client_id\`: Your App Registration (Service Principal) client ID.
  - \`tenant_id\`: Your Azure AD tenant ID.
  - \`client_secret\`: Your App Registration (Service Principal) secret.

### 4. Configure Databricks Secrets

In your Databricks workspace:

1. Go to the Databricks CLI and configure your Key Vault secrets.
2. Create a secret scope in Databricks that points to your Azure Key Vault.

### 5. Run the Notebook

Once the notebook is imported and configured:

1. Open the notebook in your Databricks workspace.
2. Update the widget values at the top of the notebook to match your Azure configurations and data source.
3. Run the notebook cells sequentially.

