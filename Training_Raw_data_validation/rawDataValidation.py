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
