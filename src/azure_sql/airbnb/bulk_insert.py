from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient

# Initialize Azure clients
credential = DefaultAzureCredential()
subscription_id = 'your-subscription-id'
resource_client = ResourceManagementClient(credential, subscription_id)
storage_client = StorageManagementClient(credential, subscription_id)

# Variables
resource_group_name = 'your-resource-group'
storage_account_name = 'your-storage-account'
role_definition_id = 'storage-blob-data-contributor-role-definition-id'
principal_id = 'managed-identity-principal-id'

# Assign role
role_assignment = storage_client.blob_containers.create_or_update_management_policies(
    resource_group_name=resource_group_name,
    account_name=storage_account_name,
    container_name="$root",
    role_assignment_name=role_definition_id,
    properties={
        'role_definition_id': role_definition_id,
        'principal_id': principal_id
    }
)
