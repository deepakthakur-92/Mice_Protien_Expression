from flask import Flask, request, render_template
from flask import Response
from flask_cors import CORS, cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from predictFromModel import prediction

app = Flask(__name__)
CORS(app)

@app.route("/",methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            path = request.json['folderPath']

            pred_val = pred_validation(path) # object initialization
            pred_val.prediction_validation() # calling the prediction_validation function
            pred = prediction(path) # object initialization

            # predicting for dataset present in database
            path = pred.predictionFromModel()
            return Response('Prediction File created at %s!!!' % path)
        else:
            path = request.form['folderPath']
            pred_val = pred_validation(path) # object initialization
            pred_val.prediction_validation() # calling the prediction_validation function
            pred = prediction(path) # object initialization

            #predicting for dataset present in database
            path = pred.predictionFromModel()
            return render_template('index.html', message='Prediction File Created at %s!!!' % path)

    except Exception as e:
        return Response("Error Occurred! %s" %e)
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)



@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            train_valObj = train_validation(path) # object initialization

            train_valObj.train_validation() #calling the training_validation function

            trainModelObj = trainModel()
            trainModelObj.trainingModel() # training the model for the files in the table

    except Exception as e:
        return Response("Error Occerred!! %s" %e)
    return Response("Training successfull!!")

if __name__=="__main__":
    app.run()
