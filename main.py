
from Classes import Database
from Models import KepClient
from Models import SetValues
import sys
from threading import Thread
sys.path.insert(0, "..")

Db = Database.Mysql('localhost',3306,'root','Korusu123','dbkepware')

Clients = Db.GetAllClients()
KepClients = []


        
         

if __name__ == '__main__':

    for client in Clients:
        cl = KepClient.KepClient(client[0])
        KepClients.append(cl)  
        cl.Connect()
        cl.CreateNodes()
        cl.AddNoErrorTagsToNodes()
        cl.SubscribeNodes()
        
    for client in KepClients:
        Thread(target = client.WriteTagsToServer).start()
    




