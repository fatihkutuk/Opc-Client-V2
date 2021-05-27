from opcua.client.client import Client
from Classes import Database
from Models import KepClient
from Models import WriteTagsToServer
import sys
sys.path.insert(0, "..")

Db = Database.Mysql('localhost',3306,'root','Korusu123','dbkepware')
Clients = Db.GetAllClients()
KepClients = []

for client in Clients:
    cl = KepClient.KepClient(client[0])
    cl.Connect()
    cl.CreateNodes()
    cl.AddNoErrorTagsToNodes()
    cl.SubscribeNodes()
    KepClients.append(cl)

WriteTagsToServer.TagYaz()