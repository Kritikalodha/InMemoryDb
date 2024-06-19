# Implement in memory DB with GET, SET and UPDATE OPERATION
from typing import List
import time
class InMemoryDB:
    def __init__(self, primary_key, columns):
        self.database = self.initialiseDB()
        self.primary_key = primary_key
        self.columns = columns

    def initialiseDB(self):
        return {}

    def checkColumn(self, column_name):
        if column_name not in self.columns:
            return False
        return True

    def setData(self, column_value) -> None:
        if not column_value.get(self.primary_key, None):
            raise Exception("Primary key not present")
            return 

        if self.database.get(column_value.get(self.primary_key, None), None):
            raise Exception("Primary key already present")
            return 
        
        for column in column_value.keys():
            if not self.checkColumn(column):
                raise Exception("Column not present")
                return 

        current_time = time.time()

        if "expiry_time" in self.columns:
            column_value["expiry_time"] = column_value["expiry_time"] + current_time

        self.database[column_value[self.primary_key]] = column_value
        return 


    def getData(self, column, value ) -> List :
        # to get column from any column
        if not self.checkColumn(column):
            raise Exception("Column not present")
            return 

        # if column is primary key
        if column == self.primary_key:
            if self.database.get(value,{}).get("expiry_time", float('inf')) < time.time() :
                return []
            return [self.database.get(value, None)]
             

        # if column is not primary key
        result  = []
        for key , di in self.database.items():
            if di[column] == value and di.get("expiry_time", float('inf'))>time.time():
                result.append(di)

        return result

    def updateData(self, on_column, on_value, update_column, new_value) -> None:

        if not self.checkColumn(on_column) or  not self.checkColumn(update_column) :
            raise Exception("Either column not present")
            return 

        # if column is primary key
        if update_column == self.primary_key:
            raise Exception("Should not update primary key")
            return

        if update_column == "expiry_time":
            new_value = time.time()+new_value

        # if column is not primary key
        result  = []
        for key , di in self.database.items():
            if di[on_column]==on_value:
                di[update_column] = new_value
        return 

    def getRowByPrefix(self, column_name, prefix):
  
        if not self.checkColumn(column_name):
            raise Exception("Column not present")
            return

        result  = []
        for key , di in self.database.items():
            if di[column_name].startswith(prefix) and di.get("expiry_time", float('inf'))>time.time() :
                result.append(di)

        return result



    def getRowBySuffix(self, column_name, suffix):
      
        if not self.checkColumn(column_name):
            raise Exception("Column not present")
            return 

        result  = []
        for key , di in self.database.items():
            if di[column_name].endswith(prefix) and di.get("expiry_time", float('inf'))>time.time():
                result.append(di)

        return result


    def showDataBase(self) -> None:
        print("DB is -> ", [ val for val in self.database.values() ])
        return 

    def deleteRowBySuffix(self, column_name, suffix) -> None:
      
        if not self.checkColumn(column_name):
            raise Exception("Column not present")
            return 

        delete_keys = []
        for key , di in self.database.items():
            if di[column_name].endswith(prefix) :
                delete_keys.append(key)

        for key in delete_keys:
            self.database.pop(key)

        return 

    def deleteRowByPrefix(self, column_name, prefix) -> None:
  
        if not self.checkColumn(column_name):
            raise Exception("Column not present")
            return

        delete_keys = []
        for key , di in self.database.items():
            if di[column_name].startswith(prefix):
                delete_keys.append(key)

        for key in delete_keys:
            self.database.pop(key)
        return 

    def deleteRow(self, column, value ) -> None :
        # to get column from any column
        if not self.checkColumn(column):
            raise Exception("Column not present")
            return 

        # if column is primary key
        if column == self.primary_key:
            if value in self.database:
                self.database.pop(value)
                return 
             

        # if column is not primary key
        delete_keys = []
        for key , di in self.database.items():
            if di[column] == value :
                delete_keys.append(key)

        for key in delete_keys:
            self.database.pop(key)

        return  

    def dumpDB(self) -> None:
        current_time = time.time()
        for key , di in self.database.items():
            if di.get("expiry_time", None):
                time_left =  di.get("expiry_time") - current_time 
                di["time_left"] = time_left

        return 

    def retrieveDB(self) -> None:
        current_time = time.time()
        for key , di in self.database.items():
            if "time_left" in di:
                di["expiry_time"] = current_time + di["time_left"]
                di.pop("time_left")

        return 


        
if __name__=="__main__": 
    try:
        db_obj = InMemoryDB("name", ['name', 'age', 'subject', 'height', 'expiry_time'])
        db_obj.setData({"name":"kritika", "age":24, "subject":"english", "height":"5.2", "expiry_time":2})
        db_obj.showDataBase()
        db_obj.setData({"name":"neha", "age":30, "subject":"hindi", "height":"5.5", "expiry_time":200 })
        db_obj.showDataBase()
        time.sleep(2)
        print(db_obj.getData("name","kritika"))
        print(db_obj.getData("name","neha"))
        db_obj.dumpDB()
        time.sleep(10)
        db_obj.retrieveDB()
        print(db_obj.getData("name","neha"))
    
    except Exception as e:
        print("error in database ->", e)


