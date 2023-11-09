from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
token_credential = DefaultAzureCredential()

account_url="https://samastercloudtrilha02dev.blob.core.windows.net/"

blob_service_client = BlobServiceClient(
    account_url=account_url,
    credential=token_credential
)

container_client = blob_service_client.get_container_client("clientes")

blob_list = container_client.list_blobs()

for blob in blob_list:
    print(blob.name + '\n')