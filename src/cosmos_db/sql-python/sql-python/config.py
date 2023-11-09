import os

settings = {
    'host': os.environ.get('ACCOUNT_HOST', 'https://mastercloud-trilha-03-cosmosdb-01.documents.azure.com:443/'),
    'master_key': os.environ.get('ACCOUNT_KEY', 'WQFSuec9BghHS5LGz5OCI4akW4FZ8dGqUA4QJ5UfzobeUwpmhbYeU61YHHBgMnr40ILVt870beQLACDbI784Xg=='),
    'database_id': os.environ.get('COSMOS_DATABASE', 'ToDoList'),
    'container_id': os.environ.get('COSMOS_CONTAINER', 'Items'),
}