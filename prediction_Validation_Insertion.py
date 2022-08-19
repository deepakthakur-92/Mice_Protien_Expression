from Prediction_Raw_data_validation.predictionDataValidation import Prediction_Data_Validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import DBOperation
from Data_Transformation_Prediction.DataTransformationPrediction import DataTransformPredict
from app_Logging import logger

class pred_validation:
    def __init__(self,path):
        self.raw_data = Prediction_Data_Validation(path)
        self.dataTransform = DataTransformPredict()
        self.dbOperation = DBOperation()
        self.dbOperation.createDatabaseForTraining('prediction_dataset')
        self.file_object = open('Prediction_Logs/Prediction_Main_Log.txt','a+')
        self.log_writer = logger.App_Logger()


    def prediction_validation(self):

        try:
            self.log_writer.log(self.file_object,'Start of Validation on files!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object,'Raw Data Validation Complete!!')

            self.log_writer.log(self.file_object,'Starting Data Transformation!!')
            # adding quotation to categorical values to insert in table
            self.dataTransform.addQuotesToStringValuesInColumn()
            # replacing blanks in the csv files with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            self.log_writer.log(self.file_object,'Data Transformation successfully!!')

            self.log_writer.log(self.file_object,
                                'Creating Training_Database and tables on the basis if given schema!!')
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dbOperation.createTableDB('good_raw_data', column_names)
            self.log_writer.log(self.file_object,'Table creation Completed!!')
            self.log_writer.log(self.file_object,'Insertion of Data into table started!!')
            # insert csv files in the table
            self.dbOperation.insertIntoTableGoodData('good_raw_data')
            self.log_writer.log(self.file_object,'Insertion in Table completed!!!')
            self.log_writer.log(self.file_object,'Deleting Good Data Folder!!!')
            # Delete the good data folder after loading files in tables
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object,'Good_Data folder deleted!!!')
            self.log_writer.log(self.file_object,'Moving bad files to Archive and deleting Bad_Data folder!!!')
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object,'Bad Files moved to archive!! Bad folder Deleted!!')
            self.log_writer.log(self.file_object,'Validation Operation completed!!')
            self.log_writer.log(self.file_object,'Extracting csv file from table')
            # export data in table to csvfile
            self.dbOperation.selectingDataFromTableIntoCSV('good_raw_data')

        except Exception as e:
            raise e