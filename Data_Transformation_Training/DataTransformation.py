from os import listdir
import pandas as pd
from app_Logging.logger import App_Logger


class DataTransform:

    """
            This class shall be used for transforming the Good Raw Data Training Data before loading it in Database!!.


            written By: Deepak Thakur
            Version: 1.0
    """

    def __init__(self):
        self.goodDataPath = "Training_raw_Data_Files_Validated/Good_Data"
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

        log_file = open("Training_Logs/dataTransformingLog.txt", 'a+')
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







