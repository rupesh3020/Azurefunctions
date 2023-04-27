import logging
import azure.functions as func
import gzip
import datetime

from ..shared.utils import write_to_storage
from ..shared.config import INGESTION_CONTAINER_NAME

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Desc : This function takes HTTP post request, takes the data from different applications, decomress it and save to landing zone

    Parameters:
            req : HTTP request which will also have lineage as payload

    Return type:
            HttpResponse: response if the data is successfully retrieved
    """
    param = req.route_params.get("parem")
    listOfParams = param.split("/",1)
    appName = listOfParams[0]
    if len(listOfParams) > 1:
        splineApiParam =  listOfParams[1]
    else:
        logging.info(appName + " ,status sent from HTTP Trigger function")
        splineApiParam =  None
        return func.HttpResponse("STATUS : wrong endpoint", status_code=500)
    req_body = req.get_body()
    if splineApiParam == "status" or splineApiParam == appName+"/status":
        return func.HttpResponse("STATUS : Everything's working 1", status_code=200, headers={"ABSA-Spline-API-Version": "1", "ABSA-Spline-Accept-Request-Encoding": "gzip"}) 
    try: 
        req_body_unzip = gzip.decompress(req_body)
        logging.warning(req_body_unzip)
    except OSError:
        logging.warning("Not a gzip type input") 
        logging.warning(req.url)
        return func.HttpResponse(
                "Input file was not compressed gzip",
                status_code=204
        )

    # Get body of request
    process_date = datetime.datetime.now().strftime('%Y-%m-%d')
    file_time = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S_%f')
    fileNameStr = "splineOutput"
    filename = appName + "-" +fileNameStr + "-" + file_time + ".json"
    blob_name = f"landingzone/spline/application={appName}/date={process_date}/{filename}"
    write_to_storage(blob_name=blob_name,data=req_body_unzip, container=INGESTION_CONTAINER_NAME)

    return func.HttpResponse(
            "Everything's working",
            status_code=200
    )