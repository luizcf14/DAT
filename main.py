import json
from DATTools import *

tools = DATTools()

jsonData = json.loads(open("config.json", "r").read())
url = jsonData["url"]
outputName = jsonData["outputName"]
tableName = jsonData["tableName"]

#fileName = "deter_public.shp"
fileName = tools.getShapeFile(url)
tools.writeNewShapeFile(fileName, outputName)
tools.sendToPostgis(outputName, tableName)