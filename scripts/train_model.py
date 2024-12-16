import joblib
from sklearn.ensemble import RandomForestClassifier


def train_model(train_x, train_y, test_x, test_y):
    rf = RandomForestClassifier(random_state=17)
    rf.fit(train_x, train_y)

    joblib.dump(rf, "./model.joblib")
