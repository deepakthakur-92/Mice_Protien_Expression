import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import LabelEncoder
import pickle
import os

class Preprocessor:
    """
        This class shall be used to clean and transform the data before training.

        Written by: Deepak Thakur
        Version: 1.0

    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def separate_label_feature(self, data, label_column_name):
        """
                Method Name: separate_label_feature
                Description: This method separates the features and a label columns.
                Output: Returns two separate Dataframes, one containing features and another contains label Columns.
                On Failure: Raise Excpetion

                Written By: Deepak Thakur
                Version: 1.0
        """

        self.logger_object.log(self.file_object, "Enteres the separate_lable_feature method of the preprocessor class")
        try:
            self.X = data.drop(labels=label_column_name, axis=1) # drop the columns specified and separate the feature columns
            self.Y = data[label_column_name] #Filter the label columns
            self.logger_object.log(self.file_object,
                                   "Label Separation Successful. Exited the separate_label_feature method of the preprocessor class")
            return self.X, self.Y

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exception occured in separate_label_feture method of the preprocessor class. Exception message: "+str(e))
            self.logger_object.log(self.file_object,
                                   "Label Separation Unsuccessful. Exited the separate_label_feature method of the preprocessor class")
            raise e

    def remove_Unnecessary_columns(self, data, columns):
        """
                Method Name: remove_Unnecessary_columns
                Description: This method removes the given columns from a pandas Dataframe
                Output: A pandas Dataframe after removing the specified columns.
                OnFailure: Raise Excepion

                Written By: Deepak Thakur
                Version: 1.0
        """

        self.logger_object.log(self.file_object, "Entered the remove_Unnecessary_columns method of the preprocessor class")
        self.data = data
        self.columns = columns

        try:
            self.useful_data = self.data.drop(labels=self.columns, axis=1) # drops the specified columns
            self.logger_object.log(self.file_object,
                                   "Column removal Successful. Exited the remove_Unnecessary_columns method of the preprocessor class")
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exception occurred in remove_Unnecessary_columns method of the preprocessor class. Exception message: "+str(e))
            self.logger_object.log(self.file_object,
                                   "Column removal Unnecessary. Exited the remove_Unnecessary_columns method of preprocessing class")
            raise e

    def is_null_present(self, data):
        """
                Method Name: is_null_present
                Description: This method checks whether there are null values present in the pandas Dataframe or not.
                Output: Returns a Boolean Value. True if null values are present in the DataFrame, False if they are not present.
                On Failure: Raise Excpetion

                Written By: Deepak Thakur
                Version: 1.0

        """
        self.logger_object.log(self.file_object, "Entered the is_null_present method of the preprocessor class ")
        self.null_present = False

        try:
            self.null_counts = data.isna().count() # check for the count of null values per column
            for i in self.null_counts:
                if i>0:
                    self.null_present=True
                    break
            if(self.null_present): # write the logs to see which columns have null values
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                preprocessing_folder = 'preprocessing_data/'
                if not os.path.isdir(preprocessing_folder):
                    os.mkdir(preprocessing_folder)
                dataframe_with_null.to_csv(preprocessing_folder+'null_values.csv', header=True, index=None) # storing the null column information to file.
            self.logger_object.log(self.file_object,'Finding missing values is successfully done, Data written to the null values file. Exited the is_null_present method of the preprocessor class')
            return self.null_present
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occurred in is_null_present method of the Preprocessor class. Exception message: ' +str(e))
            self.logger_object.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the preprocessor class')
            raise e


    def impute_missing_values(self, data):

        """
                Method Name: impute_missing_values
                Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                Output: A Dataframe which has all the missing values imputed.
                On Failure: Raise Exception

                Written By: Deepak Thakur
                Version: 1.0
        """

        self.logger_object.log(self.file_object,'Entered the impute_missing_values method of the Preprocessor class')
        self.data = data

        try:
            imputer = KNNImputer(n_neighbors=3,weights='uniform',missing_values=np.nan)
            self.new_array = imputer.fit_transform(self.data) # impute the missing values
            # convert the nd-array returned in the step above to a Dataframe
            self.new_data = pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.logger_object.log(self.file_object,'Imputing missing values Successful. Exited the impute_missing_values method of the preprocessor class')
            return self.new_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occurred in impute_missing_values method of the preprocessor class')
            self.logger_object.log(self.file_object,'Imputing missing values failed. Exited the impute_missing_values method of the preprocessor class')
            raise e


    def encode_Categorical_Values(self, data):
        """
                Method Name: encodeCategoricalValues
                Description: This method encodes all the categorical values in the training set
                Output: A Dataframe which has all the categorical values encoded.
                On Failure: Raise Exception

                Written By: Deepak Thakur
                Version: 1.0

        """

        self.data = data
        try:
            self.logger_object.log(self.file_object,'Entered encode_Categorical_Values method of Preprocessor class')
            # mapping the categorical features
            self.data['Genotype'] = self.data['Genotype'].map({'Control': 0, 'Ts650n': 1})
            self.data['Treatment'] = self.data['Treatment'].map({'Saline': 0, 'Memantine': 1})
            self.data['Behavior'] = self.data['Behavior'].map({'C/S': 0, 'S/C': 1})

            # Encoding the labels
            self.encode = LabelEncoder().fit(self.data['class'])
            self.data['class'] = self.encode.transform(self.data['class'])
            self.logger_object.log(self.file_object,'Label class encoded successfully')

            # We will save the encoder as pickle to use when we do the prediction. We will need to decode the predicted values
            encoder_folder = 'EncoderPickle/'
            if not os.path.isdir(encoder_folder):
                os.mkdir(encoder_folder)
            with open(encoder_folder+'enc.pickle','wb') as file:
                pickle.dump(self.encode, file)

            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in encode_Categorical_Values method of the preprocessor class')
            self.logger_object.log(self.file_object,
                                   'Encoding of labels values failed. Exited the encode_Categorical_Values method of the preprocessor class')
            raise e

    def encode_Categorical_Values_Prediction(self, data):
        """
                Method Name: encode_Categorical_Values_Prediction
                Description: This method encodes all the categorical values in the prediction set.
                Output: A Dataframe which has all the categorical values encoded.
                On Failure: Raise Exception

        """

        self.data = data
        try:
            self.logger_object.log(self.file_object, 'Entered encode_Categorical_Values method of Preprocessor class')
            # mapping the categorical features
            self.data['Genotype'] = self.data['Genotype'].map({'Control': 0, 'Ts650n': 1})
            self.data['Treatment'] = self.data['Treatment'].map({'Saline': 0, 'Memantine': 1})
            self.data['Behavior'] = self.data['Behavior'].map({'C/S': 0, 'S/C': 1})
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in encode_Categorical_Values_Prediction method of the preprocessor class')
            self.logger_object.log(self.file_object,
                                   'Encoding of labels values failed. Exited the encode_Categorical_Values_Prediction method of the preprocessor class')
            raise e


