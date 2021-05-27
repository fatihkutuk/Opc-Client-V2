
from Classes import Database

class SubHandler(object):#Subscribe olunan taglarda değişim olunca tetiklenir
    def __init__(self):
        self.db = Database.Mysql('localhost',3306,'root','Korusu123','dbkepware') 
        self.text = ""
        self.count = 0
    def datachange_notification(self, node, val, data):
        split_node = str(node).split('.')
        

        if(str(val)!="None"):
            if str(split_node[2])=="_System":
                self.text =self.text + "("+str(split_node[1])+",'_NoError',"+str(val)+")"+","
            else:
                self.text =self.text + "("+str(split_node[1])+",'"+str(split_node[2])+"',"+str(val)+")"+","

            
        self.count = self.count +1
        if self.count > 500:
            try:
                self.db.ReplaceIntoTagOku(self.text[:-1])
                self.count = 0
                self.text = ""
            except:
                True    