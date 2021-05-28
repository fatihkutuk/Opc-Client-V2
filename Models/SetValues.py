from Classes import Database
from opcua import ua
from opcua import Client
import time
class SetValues():
    def __init__(self):
        self.db = Database.Mysql('localhost',3306,'root','Korusu123','dbkepware')
        self.SetValuesClient = Client("opc.tcp://127.0.0.1:49320")
        self.Clients = self.db.GetAllClients()
        self.SetNodes = []
        self.SetValues = []
        while 1:
            self.SetValuesClient.connect()
            for clients in self.Clients:
                TagsToWhrite = self.db.GetTagsToWhrite(clients[0])
                try:   
                    for tag in TagsToWhrite:
                        tag = tag.fetchall()
                        for t in tag:
                            node = self.SetValuesClient.get_node("ns=2;s="+t[1]+"."+t[0]+"."+t[2])
                            varianttype = node.get_data_type_as_variant_type()
                            self.SetNodes.append(node)
                            if varianttype==ua.VariantType.Int16:
                                self.SetValues.append(ua.DataValue(ua.Variant(int(t[3]), varianttype)))
                            else:
                                self.SetValues.append(ua.DataValue(ua.Variant(t[3], varianttype)))
                    self.SetValuesClient.set_values(self.SetNodes,self.SetValues)              
                    self.SetValues.clear()
                    self.SetNodes.clear()   
                    self.SetValuesClient.disconnect() 
                except:
                    False


        

