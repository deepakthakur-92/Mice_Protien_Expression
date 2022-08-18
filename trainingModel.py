"""
    This is the Entry point for Training the Machine Learning Model.

    Written By: Deepak Thakur
    Version: 1.0
"""

# necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing, clustering
from best_model_finder import model_finder
from file_operation import file_methods
from app_Logging import logger

class trainModel:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open('Training_Logs/ModelTrainingLog.txt','a+')

    def trainingModel(self):
        # Logging the start of training
        self.log_writer.log(self.file_object,'Start of Training')
        try:
            # Getting the data from the source
            data_getter = data_loader.Data_Getter(self.file_object, self.log_writer)
            data = data_getter.get_data()


            """Data Preprocessing"""

            preprocessor = preprocessing.Preprocessor(self.file_object,self.log_writer)
            data = preprocessor.remove_Unnecessary_columns(data, ['MouseID']) # remove tthe unnamed column as it doesn't contribute to prediction

            # encode categorical data
            data = preprocessor.encode_Categorical_Values(data)

            #create separate features and labels
            X,Y = preprocessor.separate_label_feature(data,label_column_name='class')

            # check if missing values are present in the dataset
            is_null_present = preprocessor.is_null_present(X)

            # if missing values, then replace them
            if(is_null_present):
                X = preprocessor.impute_missing_values(X) # missing value imputation

            """Applying the clustering approach"""

            kmeans = clustering.KMeansClustering(self.file_object, self.log_writer) # object initialization
            number_of_clusters = kmeans.elbow_plot(X) # using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X = kmeans.create_clusters(X, number_of_clusters)

            # create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels'] = Y

            # getting the unique clusters from our dataset
            list_of_clusters = X['Cluster'].unique()

            """Parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data = X[X['Clusters']==i] # filter the data for one cluster

                # prepare the feature and label columns
                cluster_features = cluster_data.drop(['Labels','Cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1/3, random_state=355)
                model_finder = model_finder.Model_Finder(self.file_object, self.log_writer) # object initialization

                # getting  the best model for each of the clusters
                best_model_name, best_model = model_finder.get_best_model(x_train,y_train, x_test, y_test)

                # saving the best model to the directory.
                file_op = file_methods.File_Operation(self.file_object,self.log_writer)
                save_model = file_op.save_model(best_model, best_model_name +str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception as e:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object,' Unsuccessful End of Training')
            self.file_object.close()
            raise e


