import shutil
import os
import csv
from os import listdir
import pandas as pd
import mysql.connector as connection
from app_Logging import App_Logger


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


    def createDatabaseForTraining(self, Database):
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
            query = f'CREATE DATABASE IF NOT EXISTS{Database}'
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
                Output: None
                On Failure: Raise ConnectionError

                Written By: Deepak Thakur
                Version: 1.0

        """

