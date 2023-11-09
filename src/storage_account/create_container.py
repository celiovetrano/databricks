from azure.storage.blob import ContainerClient

connection_string = "DefaultEndpointsProtocol=https;AccountName=samastercloudtrilha02;AccountKey=EcL3gEQ0uqesG4AFJA2wZ5vj4ssgoRqFWmqNcFlISn93JefvS49vcbz6jUaRXTI9jsCltfwSBpo/+ASt1A0sUQ==;EndpointSuffix=core.windows.net"

client = ContainerClient.from_connection_string(
    conn_str=connection_string,
    container_name="data-oltp"
)

blob_names = client.list_blob_names()
for blob in blob_names:
    print(blob)















connection_string = "DefaultEndpointsProtocol=https;AccountName=samastercloudtrilha02dev;AccountKey=oONdVgForKa3JEkUJCXxj2nG2E3geBOrpgy05PS+qOPLvSfseVgTbQGFndqB5BISLv232QNnHp4++AStluydPw==;EndpointSuffix=core.windows.net"

container_client = ContainerClient.from_connection_string(conn_str=connection_string, container_name="clientes")

blob_names = container_client.list_blob_names()
for blob in blob_names:
    print(blob)