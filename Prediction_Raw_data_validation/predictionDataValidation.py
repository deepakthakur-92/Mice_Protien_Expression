from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from app_Logging.logger import App_Logger


class Prediction_Data_Validation:

    """
        This class is used for validation which was done on Raw Prediction Data !!.

        written by: Deepak Thakur
        Version: 1.0

    """

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
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

            file = open("Prediction_Logs/valuesfromSchemaValidation.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + "\t" + "NumberofColumns:: %s" %NumberofColumns + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
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
                Description: This method created directories to store the Good and Bad Data after validating the prediction
                             data.
                Output: None
                On Failure: OSError

                written By: Deepak Thakur
                Version: 1.0

        """
        try:
            path = os.path.join("Prediction_Raw_Data_Files_Validated/", "Good_Data/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_Data_Files_Validated/", "Bad_Data/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
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
            path = 'Prediction_Raw_Data_Files_Validated/'
            if os.path.isdir(path +'Good_Raw_Data'):
                shutil.rmtree(path + 'Good_Raw_Data')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Good Raw Data Directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
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
            path = 'Prediction_Raw_Data_Files_Validated/'
            if os.path.isdir(path + 'Bad_Data/'):
                shutil.rmtree(path + 'Bad_Data/')
                file = open('Prediction_Logs/GeneralLog.txt', 'a+')
                self.logger.log(file, "BadData directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory: %s" %s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        """
                Method Name: moveBadFilesToArchivedBad
                Description: This method deletes the directory made to store the bad Data after moving the data in an
                             archive folder. We archive the bad files to send them back to the client for invalid data issue.

                Output: None
                On Failure: OSError

                written by: Deepak Thakur
                Version: 1.0

        """

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%H%S")

        try:

            source = 'Prediction_Raw_Data_Files_Validated/Bad_Data/'
            if os.path.isdir(source):
                path = "PredictionArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                destination = 'PredictionArchiveBadData/BadData_' +str(date)+ "_"+str(time)
                if not os.path.isdir(destination):
                    os.makedirs(destination)
                files = os.listdir(source)
                for file in files:
                    if file not in os.listdir(destination):
                        shutil.move(source + file, destination)
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad files moved to Archive")
                path = 'Prediction_Raw_Data_Files_Validated/'
                if os.path.isdir(path + 'Bad_Data/'):
                    shutil.rmtree(path + 'Bad_Data/')
                self.logger.log(file, "Bad Raw Data Folder Deleted Successfully!!")
                file.close()
        except Exception as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" %e)
            file.close()
            raise e


    def validationFileNameRaw(self, regex, LenghtOfDateStampInFile, LengthOfTimeStampInFile):

        """
                Method Name: validationFileNameRaw
                Description: This function validates the name of the training csv files as per given name in the schema!
                             Regex pattern is used to do the validation. If name format do not match the file is moved
                             to Bad Raw Data folder else in Good raw data.
                Output: None
                On Failure: Exception

                Written By: Deepak Thakur
                Version: 1.0


        """

        #pattern = "['Mice_Protein_Expression']+['\_''] +[\d_]+[\d]+\.csv"

        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        # create new directories
        self.createDirectoryForGoodAndBadRawData()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open("Prediction_Logs/nameValidationLog.txt",'a+')
            for filename in onlyfiles:
                if(re.match(regex, filename)):
                    splitAtDot = resplit('.csv', filename)
                    splitAtDot = (re.split('_',splitAtDot[0]))
                    if len(splitAtDot[1]) == LenghtOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Prediction_Batch_File/" + filename, "Prediction_Raw_Data_Files_Validated/Good_Data")
                            self.logger.log(f,"Valid File name!! File moved to GoodData Folder :: %s" % filename)

                        else:
                            shutil.copy("Prediction_Batch_Files/" +filename, "Prediction_Raw_Data_files_Validated/Bad_Data")
                            self.logger.log(f,"Invalid File name!! File moved to Bad Data Folder :: %s" % filename)

                    else:
                        shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Raw_Data_files_Validated/Bad_Data")
                        self.logger.log(f, "Invalid File name!! File moved to Bad Data Folder :: %s" % filename)
                else:
                    shutil.copy("Prediction_Batch_Files/" + filename, "Prediction_Raw_Data_files_Validated/Bad_Data")
                    self.logger.log(f, "Invalid File name!! File moved to Bad Data Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Prediction_Logs/nameValidationLog.txt", "a+")
            self.logger.log(f, "Error occured while validating FileName %s" %e)
            f.close()
            raise e


    def validateColumnLength(self, NumberofColumns):

        """
                Method Name: validateColumnLength
                Description: This function validates the number of columns in the csv file.
                             It should be same as given in the schema file.
                             If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                             If the column number matches, file is kept in Good Raw Data for processing.

                Output: None
                On Failure: Exception

                written By: Deepak Thakur
                Version: 1.0

        """

        try:
            f = open("Prediction_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f,"Column Length Validation Started!!!")
            for file in listdir('Prediction_Raw_Data_Files_Validated/Good_Data'):
                csv = pd.read_csv("Prediction_Raw_Data_Files_Validated/Good_Data/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Prediction_Raw_Data_Files_Validated/Good_Data/" + file, "Prediction_Raw_Data_Files_Validated/Bad_Data")
                    self.logger.log(f, "Invalid Column Length for thr file!! File moved to Bad Raw Folder:: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Prediction_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt",'a+')
            self.logger.log(f, "Error Occured:: %s" %e)
            f.close()
            raise e
        f.close()

    def createPredictionOutputFolder(self):
        try:
            if not os.path.isdir('Prediction_Output_File'):
                os.mkdir('Prediction_Output_File')
        except Exception as e:
            raise e

    def deletePredictionFile(self):
        try:
            if os.path.exists('Prediction_Output_File/Predictions.csv'):
                os.remove('Prediction_Output_File/Predictions.csv')
        except Exception as e:
            raise e


    def validateMissingValuesInWholeColumn(self):
        """
                Method Name: validateMissingValuesInWholeColumn
                Description: This function if any column in the csv file has all value missing.
                             If all the values are missing. the file is not suitable for processing.
                             Such files are moved to bad raw data.

                Written by: Deepak Thakur
                Version: 1.0


        """
        try:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Prediction_Raw_Data_Files_Validated/Good_Data/'):
                csv = pd.read_csv("Prediction_Raw_Data_Files_Validated/Good_Data/" + file)
                for column in csv:
                    if (len(csv[column]) - csv[column].count()) == len(csv[column]):
                        shutil.move("Prediction_Raw_Data_Files_Validated/Good_Data/" +file,
                                    "Prediction_Raw_Data_Files_Validated/Bad_Data")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder:: %s" % file)
                        break
        except Exception as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt",'a+')
            self.logger.log(f,"Error Occured:: %s" %e)
            f.close()
            raise e
        f.close()










