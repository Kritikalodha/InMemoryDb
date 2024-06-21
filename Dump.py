# Implement in memory DB with GET, SET and UPDATE OPERATION
from typing import List
import time
from collections import Counter
import pytz
from datetime import datetime, timezone
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


    def setData(self, column_value, tz_name='UTC') -> bool:
        
        if not Counter([x for x in column_value.keys()]) == Counter(self.columns):
            print("Column not present")
            return False

        if self.primary_key not in column_value:
            print("Primary key not present")
            return False

        if column_value[self.primary_key] in self.database:
            print("Primary key value already present")
            return False
        
        current_time = time.time()

        if "expiry_time" in self.columns:
            current_time = datetime.now(pytz.timezone(tz_name)).astimezone(pytz.UTC)
            expiration_time = current_time.timestamp() + column_value["expiry_time"]
            column_value["expiry_time"] = expiration_time

        self.database[column_value[self.primary_key]] = column_value
        return True


    def getData(self, column, value, timestamp=None, tz_name='UTC' ) -> List :
        # to get column from any column
        if not self.checkColumn(column):
            print("Column not present")
            return []

        if timestamp is not None:
            user_time = datetime.fromtimestamp(timestamp, pytz.timezone(tz_name))
            current_time = user_time.astimezone(pytz.UTC).timestamp()
        else:
            current_time = time.time()

        # if column is primary key
        if column == self.primary_key:
            if self.database.get(value,{}).get("expiry_time", float('inf')) < current_time :
                return []

            if value in self.database:
                return [self.database[value]]
            else:
                return []

        # if column is not primary key
        result  = []
        for key , di in self.database.items():
            if di[column] == value and di.get("expiry_time", float('inf'))>current_time:
                result.append(di)

        return result

    def updateData(self, on_column, on_value, update_column_value) -> bool:
        # If on_column not present in columns list
        if not self.checkColumn(on_column) :
            print("Either column not present")
            return False

        # If update column keys not matches the schema of table
        if not Counter([x for x in update_column_value.keys()]) == Counter(self.columns):
            print("Column not present")
            return False

        # if column is primary key
        if update_column == self.primary_key:
            print("Should not update primary key")
            return False

        if update_column == "expiry_time":
            new_value = time.time()+new_value

        # if column is not primary key
        result  = []
        for key , di in self.database.items():
            if di[on_column]==on_value:
                self.database[key] = update_column_value
        return True

    def getRowByPrefix(self, column_name, prefix,timestamp=None, tz_name='UTC' ) -> list:
  
        if not self.checkColumn(column_name) :
            print("Column not present")
            return []

        if timestamp is not None:
            user_time = datetime.fromtimestamp(timestamp, pytz.timezone(tz_name))
            current_time = user_time.astimezone(pytz.UTC).timestamp()
        else:
            current_time = time.time()
        
        result  = []
        for key , di in self.database.items():
            if di[column_name].startswith(prefix) and di.get("expiry_time", float('inf'))>current_time :
                result.append(di)

        return result

    def getRowBySuffix(self, column_name, suffix, timestamp=None, tz_name='UTC') -> list:
      
        if not self.checkColumn(column_name):
            print("Column not present")
            return []

        if timestamp is not None:
            user_time = datetime.fromtimestamp(timestamp, pytz.timezone(tz_name))
            current_time = user_time.astimezone(pytz.UTC).timestamp()
        else:
            current_time = time.time()

        result  = []
        for key , di in self.database.items():
            if di[column_name].endswith(prefix) and di.get("expiry_time", float('inf'))>current_time:
                result.append(di)

        return result


    def showDataBase(self) -> None:
        print("DB is -> ", [ val for val in self.database.values() ])
        return 

    def deleteRowBySuffix(self, column_name, suffix) -> bool:
      
        if not self.checkColumn(column_name):
            print("Column not present")
            return False

        delete_keys = []
        for key , di in self.database.items():
            if di[column_name].endswith(prefix) :
                delete_keys.append(key)

        for key in delete_keys:
            self.database.pop(key)

        return True

    def deleteRowByPrefix(self, column_name, prefix) -> bool:
  
        if not self.checkColumn(column_name):
            print("Column not present")
            return False

        delete_keys = []
        for key , di in self.database.items():
            if di[column_name].startswith(prefix):
                delete_keys.append(key)

        for key in delete_keys:
            self.database.pop(key)
        return True

    def deleteRow(self, column, value ) -> bool :
        # to get column from any column
        if not self.checkColumn(column):
            print("Column not present")
            return False

        # if column is primary key
        if column == self.primary_key:
            if value in self.database:
                self.database.pop(value)
                return True
             
        # if column is not primary key
        delete_keys = []
        for key , di in self.database.items():
            if di[column] == value :
                delete_keys.append(key)

        for key in delete_keys:
            self.database.pop(key)

        return  True

    def dumpDB(self, tz_name='UTC') -> bool:
        tz = pytz.timezone(tz_name)
        # Get the current time in the specified time zone
        current_time = datetime.now(tz).timestamp()

        for key , di in self.database.items():
            
            if di.get("expiry_time", None):
                time_left =  di["expiry_time"] - current_time 
                di["time_left"] = time_left

        return True

    def retrieveDB(self, tz_name='UTC') -> bool:
        tz = pytz.timezone(tz_name)
        # Get the current time in the specified time zone
        current_time = datetime.now(tz).timestamp()
        for key , di in self.database.items():
            if "time_left" in di:
                di["expiry_time"] = current_time + di["time_left"]
                di.pop("time_left")

        return True


        
if __name__=="__main__": 
    try:
        db_obj = InMemoryDB("name", ['name', 'age', 'subject', 'height', 'expiry_time'])
        db_obj.setData({"name":"kritika", "age":24, "subject":"english", "height":"5.2", "expiry_time":2})
        db_obj.showDataBase()
        db_obj.setData({"name":"neha", "age":30, "subject":"hindi", "height":"5.5", "expiry_time":200 })
        db_obj.showDataBase()
        time.sleep(2)
        print(db_obj.getData("name","kritika", 1718998846))
        print(db_obj.getData("name","neha"))
        db_obj.dumpDB()
        time.sleep(10)
        db_obj.retrieveDB()
        print(db_obj.getData("name","neha"))
    
    except Exception as e:
        print("error in database ->", e)


