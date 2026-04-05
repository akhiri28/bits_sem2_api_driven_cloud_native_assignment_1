# Installations
# 1. pip install mlflow
# 2. pip install psutil
# psutil is a cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors) in Python

# Steps to run
# 1. In terminal, run command -> mlflow ui --host 0.0.0.0 --port 5000
# 2. Right click on main.py and "run in interactive terminal"
# 3. Open localhost:5000 in browser and see the experimental results

# Import necessary libraries
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, precision_score, recall_score, f1_score, confusion_matrix
import mlflow
import mlflow.sklearn
import psutil
import time

# Set the MLflow tracking URI to 'http'
mlflow.set_tracking_uri("http://127.0.0.1:5000")

parent_path = file_path = Path(__file__).resolve().parent.parent.parent


# Function for training the model
# Each tree in the forest has a maximum of 3 splits
# n_estimators provides number of trees in the forest, algorithm builds multiple decision trees
def train_model(model, X_train, y_train, max_depth=3, n_estimators=100, max_iter=200):
    execution_time = {}
    start_time = time.time()
    if model == 'rf':
        # Initialize the classifier
        clf = RandomForestClassifier(max_depth=max_depth, n_estimators=n_estimators, random_state=42)
    if model == 'lr':
        clf = LogisticRegression(max_iter=max_iter)
    # Train the model
    clf.fit(X_train, y_train)
    end_time = time.time()
    execution_time["system_model_training"] = end_time - start_time

    return clf, execution_time

# Function to evaluate the model
def evaluate_model(model, X_test, y_test):
    # Make predictions
    y_pred = model.predict(X_test)

    # Display classification report
    print("Classification Report:")
    print(classification_report(y_test, y_pred))


    # model metrics
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy:.2f}")
    precision = precision_score(y_test, y_pred, average='macro')
    recall = recall_score(y_test, y_pred, average='macro')
    f1 = f1_score(y_test, y_pred, average='macro')
    confusion = confusion_matrix(y_test, y_pred)

    # confusion matrix
    confusion_dict = {
        "true_positive": confusion[1][1],
        "false_positive": confusion[0][1],
        "true_negative": confusion[0][0],
        "false_negative": confusion[1][0]
    }

    # Example: CPU and Memory Usage
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent

    return {'accuracy':accuracy, 'precision': precision, 'recall': recall, 'f1': f1, 
            'confusion': confusion_dict, 'cpu_usage': cpu_usage, 'memory_usage': memory_usage}

# Function to log model and system metrics to MLflow
def log_to_mlflow(X_train, X_test, y_train, y_test,):
    
    mlflow.set_experiment("random forest experiment")
    with mlflow.start_run():

        # Train Model
        model, execution_time = train_model('rf', X_train, y_train)

        # Evaluate model and log metrics
        eval_metrics = evaluate_model(model, X_test, y_test)

        # Log hyper parameters used in Random Forest Algorithm
        mlflow.log_param("model", "Random Forest")
        mlflow.log_param("max_depth", model.max_depth)
        mlflow.log_param("n_estimators", model.n_estimators)

        # Log Model Metrics
        mlflow.log_metric("accuracy", eval_metrics['accuracy'])
        mlflow.log_metric("precision", eval_metrics['precision'])
        mlflow.log_metric("recall", eval_metrics['recall'])
        mlflow.log_metric("f1-score", eval_metrics['f1'])
        mlflow.log_metrics(eval_metrics['confusion'])

        # Log system metrics
        mlflow.log_metric("system_cpu_usage", eval_metrics['cpu_usage'])
        mlflow.log_metric("system_memory_usage", eval_metrics['memory_usage'])

        # Log execution time 
        mlflow.log_metrics(execution_time)

        # Log model
        mlflow.sklearn.log_model(model, "model")

    mlflow.set_experiment("logistic regression experiment")
    with mlflow.start_run():

        # Train Model
        model, execution_time = train_model('lr', X_train, y_train)

        # Evaluate model and log metrics
        eval_metrics = evaluate_model(model, X_test, y_test)

        mlflow.log_param("model", "LogisticRegression")
        # Log hyper parameters used in Logistic Regression Algorithm
        mlflow.log_param("max_iter", 200)

        # Log Model Metrics
        mlflow.log_metric("accuracy", eval_metrics['accuracy'])
        mlflow.log_metric("precision", eval_metrics['precision'])
        mlflow.log_metric("recall", eval_metrics['recall'])
        mlflow.log_metric("f1-score", eval_metrics['f1'])
        mlflow.log_metrics(eval_metrics['confusion'])

        # Log system metrics
        mlflow.log_metric("system_cpu_usage", eval_metrics['cpu_usage'])
        mlflow.log_metric("system_memory_usage", eval_metrics['memory_usage'])

        # Log execution time 
        mlflow.log_metrics(execution_time)

        # Log model
        mlflow.sklearn.log_model(model, "model")



# Main function
def main():
    # Load the dataset
    data = pd.read_csv(parent_path / 'data/preprocessed_data.csv')
    # Preprocess the data
    X = data.drop('Churn', axis=1)
    y = data['Churn']

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Evaluate and log to MLflow
    log_to_mlflow( X_train, X_test, y_train, y_test)

if __name__ == "__main__":
    main()
