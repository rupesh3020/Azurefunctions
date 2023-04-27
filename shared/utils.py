import logging
from os import environ
from typing import List, Optional, Tuple
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
import pyodbc

def parse_blob_path(path: Optional[str]) -> Tuple[str, str, str, str, str]:
    """
    Extracts useful information from the path of the Azure Blob that is passed
    TODO - change this into a BlobPathParser class with methods to return the specific parts of the path when necessary

    Example
    -------
    FORMAT = <container_name>/<stage>/<source_1>/<source_2>/<table_type>_<date>.csv"

    path - eg: "acquisitionlayer/landingzone/spline/RAP/20221111/reach_2021-06-17.csv"

    return: 
    Parameters
    ----------
    path - the blob path string

    Raises
    ------
    ValueError - if path is None

    Returns
    -------
    <tuple> of each of the useful parts of the path
    """
    if not path:
        raise ValueError("Expected string, received None")
    parts = path.split("/")
    zone = parts[1]
    source = parts[2]
    usecase = parts[3].split("=")[1]
    date = parts[4].split("=")[1]
    filename = parts[5]
    return (zone, source, usecase, date, filename)
    
def write_to_storage(blob_name: str, data: bytes, container: str) -> None:
    """
    Generic function to write the given data to the given blob name within Azure Storage

    The Azure Storage container to write to is set in config.py as CONTAINER_NAME

    Parameters
    ----------
    blob_name - the name of the blob to write to
    data - the data to write to the blob

    Raises
    ------
    ResourceExistsError - if the blob exists already
    """
    logging.info(blob_name+"writing to storage")
    is_local = environ.get("IS_LOCAL")
    credential = None
    if not is_local or is_local is None:
        logging.info("Running in Azure environment")
        client_id = environ["azureClientId"]
        credential = ManagedIdentityCredential(client_id=client_id)
        connection_string = "https://{}.blob.core.windows.net".format(
            environ["storageAccountName"]
        )
    else:
        logging.info("Running locally")
        connection_string = environ["AzureWebJobsStorage"]
    if credential:
        service_client = BlobServiceClient(connection_string, credential=credential)
    else:
        service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = service_client.get_container_client(container=container)
    logging.info(f"Attempting to write to {container}")
    try:
        container_client.create_container()
    except ResourceExistsError:
        logging.info(f"Container: {container} already exists")
    try:
        blob_client = service_client.get_blob_client(container, blob_name)
        # logging.warning(data)
        blob_client.upload_blob(data, overwrite=True)
    except Exception as e:
        logging.error(e)


def get_secrets_to_env(req_secrets: List[str]) -> None:
    """
    Retrieves secrets from the Azure KeyVault and if present in 'req_secrets' loads them into the environment

    Parameters
    ----------
    req_secrets - the secrets to be loaded into the environment
    """
    kv_name = environ["KEY_VAULT_NAME"]
    kv_uri = f"https://{kv_name}.vault.azure.net"
    client_id = environ["azureClientId"]
    logging.info("Loading secrets from KV to environment")
    credential = ManagedIdentityCredential(client_id=client_id)
    client = SecretClient(vault_url=kv_uri, credential=credential)
    secretlist = client.list_properties_of_secrets()
    for secret in secretlist:
        if secret.name in req_secrets:
            environ[secret.name] = client.get_secret(secret.name).value


def _initiate_purview_client():
    purview_client = None
    if purview_client is None:
        auth = ServicePrincipalAuthentication(
        tenant_id = f"some tanent id", 
        client_id = "someclient id", 
        client_secret = "some client secret"

        )

        # Create a client to connect to your service.
        client = PurviewClient(
            account_name = "somepurview",
            authentication = auth
        )
        purview_client = client
        return purview_client
    
    else:
        return purview_client
