import pandas as pd
from file_operation import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from app_Logging import logger
from Prediction_Raw_data_validation.predictionDataValidation import Prediction_Data_Validation
import pickle

class prediction:

    def __init__(self):
        self.file_object = open('Prediction_Logs/Prediction_Log.txt','a+')
        self.log_writer = logger.App_Logger()
        if path is not None:
            self.pred_data_val = Prediction_Data_Validation(path)

    def predictionFromModel(self):

        try:
            self.pred_data_val.createPredictionOutputFolder() # creates prediction output folder if not created
            self.pred_data_val.deletePredictionFile() # delete the existing prediction file from last run!
            self.log_writer.log(self.file_object,'Start of prediction')
            data_getter = data_loader_prediction.Data_Getter_pred(self.file_object,self.log_writer)
            data = data_getter.get_data()

            MouseIDs = data['MouseID']
            preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
            data = preprocessor.remove_Unnecessary_columns(data,['MouseID'])

            # get encoded values for categorical data
            data = preprocessor.encode_Categorical_Values_Prediction(data)
            is_null_present = preprocessor.is_null_present(data)
            if(is_null_present):
                data = preprocessor.impute_missing_values(data)

            file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
            kmeans = file_loader.load_model('KMeans')

            clusters = kmeans.predict(data)
            data['clusters'] = clusters
            clusters = data['clusters'].unique()
            result = []  # initialize blank list for storing predictions
            with open('EncoderPickle/enc.pickle','rb') as file: # let's load the encoder pickle file to decode the values
                encoder = pickle.load(file)

            data['MouseID'] = MouseIDs
            for i in clusters:
                cluster_data = data[data['clusters'] == i]
                mouse_ids = cluster_data['MouseID']
                cluster_data = cluster_data.drop(['clusters','MouseID'], axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                predictions = encoder.inverse_transform(model.predict(cluster_data).astype(int))
                for mouse_id, val in zip(mouse_ids, predictions):
                    result.append({'MouseID' : mouse_id, 'class': val})
            result = pd.DataFrame(result)
            path = 'Prediction_Output_File/Predictions.csv'
            result.to_csv(path,header=True, index=None) # appends result to prediction file
            self.log_writer.log(self.file_object,'End of Prediction')
        except Exception as e:
            self.log_writer.log(self.log_writer,'Error occurred while running the prediction!! Error:: %s' % ex)
            raise e
