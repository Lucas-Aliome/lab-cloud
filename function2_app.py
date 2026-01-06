import azure.functions as func
import azure.durable_functions as df
import re
from azure.storage.blob import BlobServiceClient

# Initialisation de l'application
my_app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# 1. DÉCLENCHEUR HTTP : Lance l'orchestration
@my_app.route(route="start_mapreduce")
@my_app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    instance_id = await client.start_new("MasterOrchestrator")
    return client.create_check_status_response(req, instance_id)

# 2. ORCHESTRATEUR : Gère le flux Fan-out/Fan-in
@my_app.orchestration_trigger(context_name="context")
def MasterOrchestrator(context: df.DurableOrchestrationContext):
    # Étape 1 : Récupérer les lignes des fichiers
    input_data = yield context.call_activity("GetInputDataFn", None)
    
    # Étape 2 : MAPPER (Fan-out)
    map_tasks = []
    for line in input_data:
        map_tasks.append(context.call_activity("Mapper", line))
    map_results = yield context.task_all(map_tasks)
    
    # Étape 3 : SHUFFLER
    shuffled_results = yield context.call_activity("Shuffler", map_results)
    
    # Étape 4 : REDUCER (Fan-in)
    reduce_tasks = []
    for word, counts in shuffled_results.items():
        # On envoie un dictionnaire structuré au Reducer
        reduce_tasks.append(context.call_activity("Reducer", {"word": word, "counts": counts}))
    
    final_results = yield context.task_all(reduce_tasks)
    return final_results

# 3. ACTIVITÉS

@my_app.activity_trigger(input_name="payload")
def GetInputDataFn(payload):
    # Connexion à ton compte de stockage
    connection_string = "DefaultEndpointsProtocol=https;AccountName=storemapreducealiome;AccountKey=13X4+F/+dEHYi2TfmXd67qFDrdxfUifttBsnQQHP6dhAnpTSWyigItL3pI4iHdYbt7nSUnhoDvni+AStPGggwQ==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("mapreduce-input")
    
    lines = []
    # Lecture de tous les fichiers dans le conteneur
    for blob in container_client.list_blobs():
        blob_client = container_client.get_blob_client(blob.name)
        text = blob_client.download_blob().readall().decode('utf-8')
        for content in text.splitlines():
            if content.strip():
                lines.append({"text": content})
    return lines

@my_app.activity_trigger(input_name="line")
def Mapper(line):
    # Transformation : chaque mot devient (mot, 1)
    words = re.findall(r'\w+', line["text"].lower())
    return [{"key": w, "value": 1} for w in words]

@my_app.activity_trigger(input_name="mapResults")
def Shuffler(mapResults):
    # Regroupement par mot
    shuffled = {}
    for result_list in mapResults:
        for item in result_list:
            key = item["key"]
            if key not in shuffled:
                shuffled[key] = []
            shuffled[key].append(item["value"])
    return shuffled

@my_app.activity_trigger(input_name="mapData") # Changé ici pour éviter l'erreur d'indexation
def Reducer(mapData):
    # Agrégation finale
    word = mapData.get("word")
    counts = mapData.get("counts")
    return {"word": word, "total": sum(counts)}