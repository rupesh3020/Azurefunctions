from datetime import datetime
import logging
import azure.functions as func
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient
import json
from ..shared.utils import write_to_storage, parse_blob_path, get_secrets_to_env, _initiate_purview_client
from ..shared.config import CURATED_CONTAINER_NAME,INGESTION_CONTAINER_NAME
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess, TypeCategory, AtlasAttributeDef
import os
import copy

guidTracker = GuidTracker(-1000)
upload_entities1 = []


def generate_column_mapping(input_table,output_table):
    """
    This can use a algorithm to find the leaf node from reads table to output table for columns connections 
    """
    col_map = [
    {
        "DatasetMapping": {
        "Source": input_table.qualifiedName, "Sink": output_table.qualifiedName},
        "ColumnMapping": [
        {"Source": "agreementid", "Sink": "BrandName"},
        {"Source": "commercialcredittypeid", "Sink": "GlobalFacilityIdentifier"}]  
    }
        ]
    return col_map



def create_column_relation(client:PurviewClient,entity:AtlasEntity,column_data:dict):
    x = entity.to_json()
    column_entity_arr = []
    for column in column_data:
        name = column["columnName"]
        column_entity = AtlasEntity(
            name,
            "spark_column",
            qualified_name = x["attributes"]["qualifiedName"] + f"#{name}",
            guid=guidTracker.get_guid(),
            attributes = {
                "type":"string",
                "description": "This is my column added via the API"
            }    )
        column_entity.addRelationship(table=entity)
        column_entity_arr.append(column_entity)
    return column_entity_arr

def _create_dataset_level_lineage(json,client):
    pass

def _get_entity(client:PurviewClient ,qualified_name: str):
    
    if qualified_name.startswith("https://"):
        qualified_name= qualified_name + "/{SparkPartitions}"
        logging.warning(qualified_name)
        query_filter = {
                    "attributeName": "qualifiedName",
                    "operator": "eq",
                    "attributeValue": qualified_name
                }
        
        entities = client.discovery.query(filter=query_filter)
        logging.warning(entities)
        if entities['@search.count']  == 0:
            return None
        entity_requested = client.get_entity(typeName="azure_datalake_gen2_resource_set",qualifiedName=qualified_name)
        logging.info(entity_requested)
        if len(entity_requested)>0:
            entity_requested_obj = AtlasEntity.from_json(entity_requested["entities"][0])
            return entity_requested_obj
        else:
            return None
    else:
        return None

def create_table_entity(table: dict):
    attributes = {
        "id" : table["id"],
        "name": table["name"],
        "type": table["type"],
        "linked_transformation_id": table["linkedTransformationId"]
    }
    entity =  AtlasEntity(  name = table["name"],
                            typeName = "spark_table",
                            qualified_name = table["qualifiedpath"],
                            guid = guidTracker.get_guid()
                            )
    entity.addCustomAttribute(**attributes)
    columns = create_columns_for_entity(entity,table["schema"])
    upload_entities1.append(columns)
    entity.addRelationship(columns=[ c.to_json(minimum=True) for c in columns])
    return entity

def create_columns_for_entity(entity,schema):
    column_entity_arr = []
    entity_json = entity.to_json()
    for column in schema:
        name = column["columnName"]
        column_entity = AtlasEntity(
            name,
            "spark_column",
            qualified_name = entity_json["attributes"]["qualifiedName"] + f"#{name}",
            guid=guidTracker.get_guid(),
            attributes = {
                "type":"string",
                "columnId": column["columnId"]
            }    )
        column_entity.addRelationship(table=entity)
        column_entity_arr.append(column_entity)
    return column_entity_arr

def convert_Spline_to_Purview(client:PurviewClient ,splineJson):
    splineJson = json.loads(splineJson)

    # Get inputs
    only_input_tables = [] 
    inputs = []
    outputs = []
    input_colunms_arr =[]
    for read in splineJson["inPutTable"]: 
        input_path = read["qualifiedpath"]
        input = _get_entity(client=client,qualified_name=input_path)
        if input is not None:
            inputs.append(input)
        else:
            input = create_table_entity(read)
            inputs.append(input)
            input_columns = create_column_relation(client=client,entity=input,column_data=read["schema"])
            input_colunms_arr = input_colunms_arr + input_columns
    only_input_tables = copy.deepcopy(inputs)
    inputs = inputs + input_colunms_arr
    output_path = splineJson["outPutTable"]["qualifiedpath"]
    output=_get_entity(client=client,qualified_name=output_path)
    if output is None:
        output = create_table_entity(splineJson["outPutTable"])
        output_columns = create_column_relation(client=client,entity=output,column_data=splineJson["outPutTable"]["schema"])
        outputs = outputs + output_columns
    outputs.append(output)
    
    # Get Process 
    process_attributes = {
                        "name": splineJson["processId"],
                        "owner":splineJson["applicationShortName"] ,
                        "description": f"Link to spark job notebook: http://someurl",
                        "startTime": splineJson["eventTimestamp"],
                        "endTime": 131232434
                        }
    process = AtlasProcess(
                            name = splineJson["processId"],
                            typeName = "azure_synapse_operation",
                            qualified_name = f"{splineJson['processName']}_spark_process",
                            inputs = only_input_tables,
                            outputs = [output],
                            guid = guidTracker.get_guid(),
                            attributes = process_attributes
                            )
    col_map = generate_column_mapping(only_input_tables[0],output)
    process.attributes.update({"columnMapping": json.dumps(col_map)})

    purview_lineage =  inputs + outputs + [process]
    return purview_lineage

def main(myblob: func.InputStream) -> None:
    """
    Desc : main function will be used to get spline lineage data as input binding and save 
            output after transforming it to common model
    parameters: 
        myblob : input binding will be used to get data when ever HTTP function writes to landing zone
    Return Type : None
    """
    if myblob.length is None or myblob.length==0:
            logging.info("The blob has no data in it.")
            logging.info("The blob is most likely a folder.")
            return
    list_of_secrets = ["sqluser","environment","sqlpassword"] 
    is_local = os.environ.get("IS_LOCAL")
    if not is_local or is_local is None:
        get_secrets_to_env(list_of_secrets)
    else:
        logging.warning("running in local")
    currentDT: str = (datetime.now().strftime("%Y%m%d%H%M%S"))
    logging.info('Python Blob trigger function processed %s', myblob.name)
    file = myblob.read()
    filepath = myblob.name
    zone, source, usecase, date, filename = parse_blob_path(filepath)

    purview_client = _initiate_purview_client()
    entities = convert_Spline_to_Purview(client=purview_client,splineJson=file)
    x=purview_client.collections.upload_entities(batch=entities, collection="soemname")
    print(x)
    logging.warning(json.dumps(x,indent=2))
  
