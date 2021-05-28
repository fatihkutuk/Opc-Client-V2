
from Classes import Database
from opcua import ua
from opcua import Client
from . import SubHandler

class KepClient():
    def __init__(self,clientId):
        self.db = Database.Mysql('localhost',3306,'root','Korusu123','dbkepware')
        self.client = Client("opc.tcp://127.0.0.1:49320")
        self._clientId = clientId
        self.nodes = []
        self.handler = SubHandler.SubHandler()
        self.handle = None
        self.sub = None
        self.NodesToWrite = []
        self.ValuesToWrite =[]

    def Connect(self):
        self.client.connect()

    def CreateNodes(self):
        client_nodes = self.db.GetClientSubsciptionList(self._clientId)
        for client_node in client_nodes:
            client_node = client_node.fetchall()
            for node in client_node:
                try:
                    self.nodes.append(self.client.get_node("ns=2;s="+node[1]+"."+node[2]+"."+node[3]))
                except:
                    pass    
    def AddNoErrorTagsToNodes(self):
        devices = self.db.GetDevicesByClientId(self._clientId)
        for device in devices:
            try:
                self.nodes.append(self.client.get_node("ns=2;s="+device[0]+"."+str(device[1])+"._System._NoError"))     
            except:
                pass
    def SubscribeNodes(self):
        self.sub = self.client.create_subscription(500,self.handler)
        try:
            self.handle = self.sub.subscribe_data_change(self.nodes)
        except:
            pass  
    def WriteTagsToServer(self):
        SetNodes = []
        SetValues = []
        while 1:
            try:
                TagsToWhrite = self.db.GetTagsToWhrite(self._clientId)
            except:
                False    
            for tag in TagsToWhrite:
                tag = tag.fetchall()
                for t in tag:
                
                    node = self.client.get_node("ns=2;s="+t[1]+"."+t[0]+"."+t[2])
                    try:
                        varianttype = node.get_data_type_as_variant_type()
                        SetNodes.append(node)
                        if varianttype==ua.VariantType.Int16:
                            SetValues.append(ua.DataValue(ua.Variant(int(t[3]), varianttype)))
                        else:
                            SetValues.append(ua.DataValue(ua.Variant(t[3], varianttype)))

                    except Exception as e :
                        True

            if len(SetValues)>0:
                try:
                    self.client.set_values(SetNodes,SetValues)

                except:
                    True    
            SetValues.clear()
            SetNodes.clear()    
        #         SetNodes.append(node)
        #         varianttype = node.get_data_type_as_variant_type()
        #         if varianttype==ua.VariantType.Int16:
        #             dv = ua.DataValue(ua.Variant(int(t[3]), varianttype))
        #         else:    
        #             dv = ua.DataValue(ua.Variant(t[3], varianttype))
        #         SetValues.append(dv)

        # try:
        #     if len(SetValues)>0:
        #         self.client.set_values(SetNodes,SetValues)
        #         SetNodes.clear()
        #         SetValues.clear()   
        # except Exception as e:
        #     print(e)   

             
    def UnsubscribeNodes(self):
        self.sub.unsubscribe(self.handle)

    def Disconnect(self):
        self.client.disconnect()    