from os import listdir
import pandas as pd
from app_Logging.logger import App_Logger


class DataTransformPredict:

    """
            This class shall be used for transforming the Good Raw prediction Data before loading it in Database!!.


            written By: Deepak Thakur
            Version: 1.0
    """

    def __init__(self):
        self.goodDataPath = "Prediction_raw_Data_Files_Validated/Good_Data"
        self.logger = App_Logger()


    def replaceMissingWithNull(self):
        """
                Method name: replaceMissingWithNull
                Description: This method replaces the missing values in column with "NULL" to store in the table.
                             We are using substring in the first column to keep only "Integer" data for ease up the loading.
                             This column is anyways going to be removed during training

                Output: None
                On Failure: Exception

                Written By: Deepak Thakur
                Version: 1.0

        """

        log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                csv = pd.read_csv(self.goodDataPath+ "/" +file)
                csv.fillna('NULL', inplace=True)
                csv.to_csv(self.goodDataPath+ "/" +file, index=None, header=True)
                self.logger.log(log_file, "%s: File Transformed successfully!!" % file)
        except exception as e:
            self.logger.log(log_file,"Data transformation failed because:: %s" %e)
            log_file.close()
        log_file.close()


    def addQuotesToStringValuesInColumn(self):
        """
                Method Name: addQuotesToStringValuesInColumn
                Description: This method converts all the columns with string datatype such that each value for the column
                             is enclosed in quotes. This is done to avoid the error while inserting string values in table
                             as varchar.
                Output: None
                On Failure: Exception

                Written By: Deepak Thakur
                Version: 1.0

        """

        log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
        try:
            columns = ['MouseID','Genotype','Treatment','Behavior','class']

            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data = pd.read_csv(self.goodDataPath+'/'+file)
                # list of columns with string datatype variables

                for col in data.columns:
                    if col in columns: # add quotes in string value
                        data[col] = data[col].apply(lambda x: "'"+str(x)+"'" if not pd.isna(x) else x)

                data.to_csv(self.goodDataPath+"/"+file, index=None, header=True)
                self.logger.log(log_file,'%s: Quotes added successfully!!' % file)
        except Exception as e:
            self.logger.log(log_file,'Data Transformation failed because:: %s'% e)
            raise e







