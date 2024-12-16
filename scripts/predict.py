import joblib


def predict(X):
    model = joblib.load("./model.joblib")
    y = model.predict(X)
    return y
