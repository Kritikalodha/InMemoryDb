# Implement in memory DB with GET, SET and UPDATE OPERATION
from typing import List

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

        self.database[column_value[self.primary_key]] = column_value
        return 


    def getData(self, column, value ) -> List:
        # to get column from any column
        if not self.checkColumn(column):
            raise Exception("Column not present")
            return 

        # if column is primary key
        if column == self.primary_key:
            raise [self.database.get(value, None)]
            return 

        # if column is not primary key
        result  = []
        for key , di in self.database.items():
            if di[column] == value:
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

        # if column is not primary key
        result  = []
        for key , di in self.database.items():
            if di[on_column]==on_value:
                di[update_column] = new_value
        return 

    def showDataBase(self) -> None:
        print("DB is -> ", self.database)
        return 
        
        
if __name__=="__main__": 
    try:
        db_obj = InMemoryDB("name", ['name', 'age', 'subject', 'height'])
        db_obj.setData({"name":"kritika", "age":20, "subject":"english", "height":"5.2"})
        db_obj.showDataBase()
        db_obj.setData({"name":"suraj", "age":50, "subject":"hindi", "height":"5.0"})
        db_obj.showDataBase()
        # db_obj.setData({"name":"suraj", "age":50, "subject":"hindi", "height":"5.0"})
        db_obj.updateData("name", "suraj", "age",30)
        db_obj.showDataBase()
       
    
    except Exception as e:
        print("error in database ->", e)



# input [[SET, {column1:values1, column2:value2, column3:value3 }]]
# input [[GET, {column1:value1, column2:value2}]]
# input [[UPDATE, {column1:value1, column2:value2}]]




    

