#### This README file is a designed and visualised Architecture of predictions of hotel booking cancelletions based on Kaggle's public `Hotel Booking Demand` dataset. 
##### It includes necessary diagrams and short descriptions toprovide clarity.
### 1. Components 
- **API**: for getting predictions
- **Random Forest Classifier AI Model**: predicts if booking will be cancelled
- **Raw dataset**: Kaggle public `Hotel Booking Demand` dataset
- **Cloud database**: `Files.io` MySQL cloud database for storing the clean dataset
### 2. Data Flow
##### Diagram
![DataFlow diagram in diagrams/DataFlow.png](diagrams/DataFlow.png)
##### Description
- Initially the raw dataset is loaded from Kaggle Public Datasets.
- In data preparation step data is cleaned, normalized and standartized. 
- After cleaning clean data is saved to the `Filess.io` MySQL database. 
- The data is loaded again from the cloud database and used for training.
- The trained model is then saved and stored in .h5 file.
- Predictions are possible through the API.
### 3. System Architecture Design 
##### Diagram
![SystemArchitecture diagram in diagrams/SystemArchitecture.png](diagrams/SystemArchitecture.png)
##### Description
- Initially the raw dataset is loaded from Kaggle Public Datasets.
- The data in a form of pandas Dataframe is prepared to training using prepare_data.py.
- Prepared clean data is in a form of a Dataframe.
- Clean data is then saved to the `Filess.io` MySQL database.
- Clean data is loaded again from the cloud database and used for training the model using train_model.py.
- The trained model is then saved and stored locally in .h5 file.
- Prediction requests can be made through the API and using predict.py.