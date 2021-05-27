from opcua import ua
from opcua import Client
from Classes import Database
import time
        
db = Database.Mysql('host',3306,'user','pass','dbname')
AllClients =  db.GetAllClients() # veritabanındaki clientları getirir

def TagYaz():
    TagYazClient = Client("opc.tcp://127.0.0.1:49320")

    while 1:

        TagYazClient.connect()
        for clients in AllClients:
            SetNodes = []
            SetValues = []
            try:
                TagsToWhrite = db.GetTagsToWhrite(clients[0])
            except:
                False    
            for tag in TagsToWhrite:
                tag = tag.fetchall()
                for t in tag:
                    try:
                        node = TagYazClient.get_node("ns=2;s="+t[1]+"."+t[0]+"."+t[2])
                        SetNodes.append(node)
                        varianttype = node.get_data_type_as_variant_type()
                        if varianttype==ua.VariantType.Int16:
                            dv = ua.DataValue(ua.Variant(int(t[3]), varianttype))
                        else:    
                            dv = ua.DataValue(ua.Variant(t[3], varianttype))
                        SetValues.append(dv)
                    except Exception as e:
                        True
            try:
                if len(SetValues)>0:
                    TagYazClient.set_values(SetNodes,SetValues)
                    SetNodes.clear()
                    SetValues.clear()   
            except:
                False    
        TagYazClient.disconnect()
        time.sleep(0.2)