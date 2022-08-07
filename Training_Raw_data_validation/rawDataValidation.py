from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from app_Logging.logger import App_Logger


class Raw_Data_Validation:

    """
        This class is used for validation which was done on Raw Training Data !!.

        written by: Deepak Thakur
        Version: 1.0

    """

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()


    def valuesFromSchema(self):

        """
                Method Name: valuesFromSchema
                Description: This method extracts all the relevant information from the pre-defined "schema" file.
                Output: LenghtOfDateStampInFile, LenghtOfTimeStampInFile, column_names, Number of columns
                On Failure: Raise ValueError, KeyError, Exception

                Written by: Deepak Thakur
                Version: 1.0

        """

        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()

            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidation.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + "\t" + "NumberofColumns:: %s" %NumberofColumns + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):

        """
                Method Name: maualRegexCreation
                Description: This method contains a manually defined regex based on the "FileName" given in "schema" file.
                             This regex is used to validate the filename of the training data.
                Output: Regex pattern
                On Failure: None

                written by: Deepak Thakur
                Version: 1.0

        """

        regex = "['Mice_Protein_Expression']+['\_''] +[\d_]+[\d]+\.csv"
        return regex


    def createDirectoryForGoodAndBadRawData(self):

        """
                Method Name: createDirectoryForGoodAndBadRawData
                Description: This method created directories to store the Good and Bad Data after validating the training data.
                Output: None
                On Failure: OSError

                written By: Deepak Thakur
                Version: 1.0

        """
        try:
            path = os.path.join("Training_Raw_Data_Files_Validated/", "Good_Data/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_Data_Files_Validated/", "Bad_Data/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Directory %s:" % ex)
            file.close()
            raise OSError


    def deleteExistingGoodDataTrainingFolder(self):

        """

                Method Name: deleteExistingGoodRawDataTrainingFolder
                Description: This method deletes the directory made to store the Good Raw Data after loading the data
                             in the table. Once the good files are loaded in the DB, deleting the directory ensures
                             space optimization.
                Output: None
                On Failure: OSError

                Written By: Deepak Thakur
                Version: 1.0

            """

        try:
            path = 'Training_Raw_Data_Files_Validated/'
            if os.path.isdor(path +'Good_Raw_Data'):
                shutil.rmtree(path + 'Good_Raw_Data')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Good Raw Data Directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError


    def deleteExistingBadDataTrainingFolder(self):

        """
                Method Name: deleteExistingBadDataTrainingFolder
                Description: This method deletes the directory made to store the bad Data.
                Output: None
                On Failure: OSError

                written by: Deepak Thakur
                Version: 1.0

        """

        try:
            path = 'Training_Raw_Data_Files_Validated'



