import shutil
import os
import csv
from os import listdir
import pandas as pd
import mysql.connector as connection
from app_Logging.logger import App_Logger


class DBOperation:
    """
        This class shall be used for handling all the SQL operations.

        Written By: Deepak Thakur
        Version: 1.0

    """

    def __init__(self):
        self.badFilePath = "Training_Raw_Data_Files_Validated/Bad_Data"
        self.goodFilePath = "Training_Raw_Data_Files_Validated/Good_Data"
        self.logger = App_Logger()


    def createDatabaseForTraining(self, DatabaseName):
        """
                Method Name: createDatabaseForTraining
                Description: This method creates database for training operation i.e storing data in database
                Output: None
                On Failure: Exception

                Written By: Deepak Thakur
                Version: 1.0

        """

        try:
            conn = connection.connect(host="localhost", user="root", passwd="root", use_pure=True)
            query = f'CREATE DATABASE IF NOT EXISTS{DatabaseName}'
            cursor = conn.cursor()
            cursor.execute(query)
            conn.close()
        except Exception as e:
            raise e


    def dataBaseConnection(self, DataBase):

        """
                Method Name: dataBaseConnection
                Description: This method creates the database with the given name and if Database already exists then opens
                             the connection to the DB.
                Output: DataBase Connection
                On Failure: Raise ConnectionError

                Written By: Deepak Thakur
                Version: 1.0

        """

        try:
            file = open("Training_Logs/DataBaseConnectionLog.txt",'a+')
            conn = connection.connect(host="localhost", database=DataBase, user="root", password="root", use_pure=True)
            self.logger.log(file, "Opened %s database successfully" % DataBase)
            file.close()
        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn


    def createTableDB(self, TableName, column_names):
        """
                Method Name: createTableDB
                Description: This method creates a table in the given database which will be used to insert the
                             Good Data after raw data validation.
                Output: None
                On Failure: Raise Exception

                Written By: Deepak Thakur
                Version: 1.0

        """

        db_Name = 'training_dataset'

        try:
            conn = self.dataBaseConnection(db_Name)
            query = f'SHOW TABLES IN {db_Name}'
            cursor = conn.cursor()
            cursor.execute(query)
            present_tables = [i[0] for i in cursor.fetchall()]

            if TableName in present_tables:
                file = open('Training_Logs/DbTableCreationLog.txt','a+')
                self.logger.log(file, "Table already created!!")
                file.close()

                conn.close()

                file  = open('Training_Logs/DataBaseConnectionLog.txt','a+')
                self.logger.log(file, "Closed %s database successfully!!" % db_Name)

            else:
                for column_name in column_names.keys():
                    datatype = column_names[column_name]

                    #in try block we check if the table exist, if yes then add columns to the table
                    #else in catch block  we will create the table

                    try:
                        cursor = conn.cursor()
                        cursor.execute(f'ALTER TABLE {TableName} ADD COLUMN {column_name} {datatype};')
                    except:
                        cursor = conn.cursor()
                        cursor.execute(f'CREATE TABLE {TableName} ({column_name} {datatype});')

                conn.close()

                file = open("Training_Logs/DbTableCreationLog.txt",'a+')
                self.logger.log(file,"Table created successfully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt",'a+')
                self.logger.log(file, "Closed %s database successfully!!" % db_Name)
                file.close()

        except Exception as e:
            file = open('Training_Logs/DbTableCreationLog.txt','a+')
            self.looger.log(file, "Error while creating table: %s" %e)
            file.close()
            conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file, "Closed %s database successfully" % db_Name)
            file.close()
            raise e


    def insertIntoTableGoodData(self, TableName):

        """
                Method Name: insertIntoTableGoodData
                Description: This method inserts the Good data files from the Good_Raw folder into the above
                             created table.
                Output: None
                On Failure: Raise Exception

                Written By: Deepak Thakur
                Version: 1.0

        """
        db_Name = 'training_dataset'

        conn = self.dataBaseConnection(db_Name)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open('Training_Logs/DBInsertLog.txt','a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file,'r') as f:
                    next(f)
                    reader = csv.reader(f, delimeter="\n")
                    for line in reader:
                        try:
                            cursor = conn.cursor()
                            cursor.execute(f'INSERT INTO {TableName} VALUES ({line[0]});')
                            self.logger.log(log_file, " %s: File loaded successfully!!" % file)
                            conn.commit()
                        except Exception as e:
                            raise e
            except Exception as e:
                self.logger.log(log_file, "Error while inserting the data into table" % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file,"File Moved Successfully %s" % file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()


    def selectingDataFromTableIntoCSV(self, TableName):

        """
                Method Name: selectingDataFromTableIntoCSV
                Description: This method exports the data in GoodData table as a CSV file in a given location.
                             above created.
                Output: None
                OnFailure: Raise Exception

                Written BY: Deepak Thakur
                Version: 1.0
        """

        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        db_name = 'training_dataset'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')

        try:
            #Make the CSV output directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            conn = self.dataBaseConnection(db_name)
            query = f'SELECT * FROM {TableName};'
            df = pd.read_sql(query, conn)
            df.to_csv(self.fileFromDb + self.fileName, header=True, index=None)

            self.logger.log(log_file, "File exported successfully!!!")
            conn.close()
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()








