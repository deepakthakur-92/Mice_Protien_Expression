from sklearn.ensemble import RandomForestClassifer
from xgboost import XGBClassifer
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_auc_score, accuracy_score

class Model_Finder:
    """
            This class will be used to find the model with best accuracy and AUC score
            Written By: Deepak Thakur
            Version: 1.0
    """

    def __init__(self, file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.clf = RandomForestClassifer()
        self.xbg = XGBClassifer(objective='binary:logistic')

    def get_best_params_for_random_forest(self, train_x, train_y):
        """
                Method Name: get_best_params_for_random_forest
                Description: get the parameters for Random forest Algorithm which give the best accuracy.
                             Use HyperParameter tuning.
                Output: The model with the best parameters
                On Failure: Raise Exception

                Written By: Deepak Thakur
                Version: 1.0

        """

        self.logger_object.log(self.file_object,'Entered the get_best_params_for_random_forest method of the Model_Finder class')
        try:
            # initializing with different combination of parameters
            self.param_grid = {"n_estimator" : [10,50,100,130], "criterion" : ['gini','entropy'],
                               "max_depth" : range(2,4,1), "max_features": ['auto','log2']}

            # Creating an abject of the Grid Search class
            self.grid = GridSearchCV(estimator=self.clf, param_grid=self.param_grid, cv=5, verbose=3)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']
            self.n_estimators = self.grid.best_params_['n_estimators']

            # Creating a new model with best parameters
            self.clf = RandomForestClassifer(n_estimators = self.n_estimators,criterion=self.criterion,
                                             max_depth=self.max_depth, max_features=self.max_features)

            # training the new model
            self.clf.fit(train_x,train_y)
            self.logger_object.log(self.file_object,
                                   'Random Forest best params: '+str(self.grid.best_params_)+'. Exited the get_best_params_for_random_forest method of the Model_Finder class')
            return self.clf
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in get_best_params_for_random_forest of the Model_Finder class. Exception message: '+str(e))
            self.logger_object.log(self.file_object,
                                   'Random Forest Parameter tuning failed. Exited the get_best_params_for_random_forest method of the Model_Finder class')
            raise e


    def get_best_params_for_xgboost(self, train_x, train_y):
        """
                Method Name: get_best_params_for_xgboost
                Description: get the parameters for xgboost Algorithm which give the best accuracy.
                             Use hyper Parameter Tuning.
                Output: The model with the best parameter
                On Failure: Raise Exception

                Written By: Deepak Thakur
                Version: 1.0
        """

        self.logger_object.log(self.file_object,'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        try:
            # Initializing with different combination of parameters
            self.param_grid_xgboost = {
                'learning_rate': [0.5,0.1,0.01,0.001],
                'max_depth' : [3,5,10,20],
                'n_estimators' : [10,50,100,200]
            }

            # Creating an object of the Grid Search class
            self.grid = GridSearchCV(self.xgb, self.param_grid_xgboost, verbose=3, cv=5)
            # finding the best parameters
            self.grid.fit(train_x, train_y)

            # extracting the best parameters
            self.learning_rate = self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            # Creating a new model with the best parameters
            self.xgb = XGBClassifer(objective='multi:softprob',learning_rate=self.learning_rate, max_depth=self.max_depth, n_estimators=self.n_estimators)
            # training the new model
            self.xgb.fit(train_x,train_y)
            self.logger_object.log(self.file_object,
                                   'XGBoost best params: '+str(self.grid.best_params_)+'. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in get_best_params_for_xgboost method of the Model_Finder class.Exception message: ' +str(e))
            self.logger_object.log(self.file_object,
                                   'XGBoost Parameter tuning failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise e


    def get_best_model(self, train_x, train_y, test_x, test_y):
        """
                Method Name: get_best_model
                Description: Find out the Model which has the best AUC score.
                Output: The best model name and the model object
                On Failure: Raise Exception

                Written By: Deepak Thakur
                Version: 1.0
        """

        self.logger_object.log(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        # create best model for XGBoost
        try:
            self.xgboost = self.get_best_params_for_xgboost(train_x,train_y)
            # We will be using predict_proba in case of a mulitclass classification as roc_auc_score needs predict_proba to calculate the score
            self.prediction_xgboost = self.xgboost.predict(test_x) # predictions using the XGBoost Model

            if len(test_y.unique()) == 1: # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                self.logger_object.log(self.file_object,'Accuracy for XGBoost: '+str(self.xgboost_score)) # Log AUC
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost) # AUC for XGBoost
                self.logger_object.log(self.file_object,'AUC for XGBoost:' +str(self.xgboost_score)) # Log AUC

            # create best model for Random forest
            self.random_forest = self.get_best_params_for_random_forest(train_x,train_y)
            self.prediction_random_forest = self.random_forest.predict_proba(test_x) # prediction using the Random Forest Algorithm

            if len(test_y.unique()) == 1: # if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
                self.random_forest_score = accuracy_score(test_y, self.prediction_random_forest)
                self.logger_object.log(self.file_object,'Acuracy for RF:'+str(self.random_forest_score))
            else:
                self.random_forest_score = roc_auc_score(test_y, self.prediction_random_forest) # AUC for Random Forest
                self.logger_object.log(self.file_object,'AUC for RF:'+str(self.random_forest_score))

            # comparing the two models
            if(self.random_forest_score < self.xgboost_score):
                return 'XGBoost', self.xgboost
            else:
                return 'RandomForest', self.random_forest

        except Excpetion as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in get_best_model method of the Model_Finder class. Exception message: '+str(e))
            self.logger_object.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise e
