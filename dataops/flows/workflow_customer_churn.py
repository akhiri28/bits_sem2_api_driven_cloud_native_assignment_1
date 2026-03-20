import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
import numpy as np
from sklearn.preprocessing import OrdinalEncoder

# print(os.path.dirname(__file__))
# raw_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'Telco-Customer-Churn.csv')
# print(os.path.normpath(raw_path))

# print(pd.read_csv(os.path.normpath(raw_path)).head())



# .parent is the equivalent of dirname
# .parent.parent moves you up one more level (the '..' equivalent)
# file_path = Path(__file__).resolve().parent.parent.parent / "data" / "Telco-Customer-Churn.csv"
parent_path = file_path = Path(__file__).resolve().parent.parent.parent
# print(file_path)
# print(pd.read_csv(file_path).head(1))

# Step 2: Load the Dataset
@task
def load_dataset():
    # Load the dataset -- 768 rows of patient data
    file_path = parent_path / "data" / "Telco-Customer-Churn.csv"
    # print(file_path)
    return pd.read_csv(file_path)

# Step 3: Data Preprocessing
@task(log_prints=True)
def preprocess_data(df):
    # Handle the empty spaces in the dataframe
    is_empty_space = df.apply(lambda x: x.astype(str).str.strip() == "").any()
    print("Columns with empty strings/spaces:")
    print(is_empty_space[is_empty_space == True])
    # Fill empty space with null value
    df[is_empty_space[is_empty_space == True].index[0]] = df[is_empty_space[is_empty_space == True].index[0]].apply(lambda x: np.nan if x.strip() == "" else x )

    # Print columns with missing values and their count
    missing_values = df.isna().sum()
    columns_with_missing = missing_values[missing_values > 0]
    df[columns_with_missing.index] = df[columns_with_missing.index].fillna(0)
    print("Columns with missing values: ")
    print(columns_with_missing)

    # Replace with Median value
    # df.fillna(df.median(), inplace=True)

    # # Normalize using Min-Max Scaling
    # scaler = MinMaxScaler()
    # features = df.drop('class', axis=1)  # Exclude the target variable
    # df_normalized = pd.DataFrame(scaler.fit_transform(features), columns=features.columns)
    # df_normalized['class'] = df['class']  # Add the target variable back to the dataframe

    # # Print the normalized dataframe
    # print("Normalized DataFrame:")
    # print(df_normalized.head())  # Printing only the first few rows for brevity

    # Binning
    df['TotalCharges']= df['TotalCharges'].astype(float)
    # Divide data into 3 equal-sized groups (Quartiles)
    df['MonthlyCharges_bin'] = pd.qcut(df['MonthlyCharges'], q=3, labels=['Low', 'Medium', 'High'])
    df['TotalCharges_bin'] = pd.qcut(df['TotalCharges'], q=3, labels=['Low', 'Medium', 'High'])
    df['tenure_bin'] = pd.qcut(df['tenure'], q=3, labels=['Low', 'Medium', 'High'])

    # Normalization
    cols_to_be_normlaized = ['MonthlyCharges', 'TotalCharges', 'tenure']
    df['TotalCharges']= df['TotalCharges'].astype(float)
    # Vectorized operation across the subset
    df[cols_to_be_normlaized] = (df[cols_to_be_normlaized] - df[cols_to_be_normlaized].min()) / (df[cols_to_be_normlaized].max() - df[cols_to_be_normlaized].min())
    df[cols_to_be_normlaized]

    # Encoding
    # Select bool columns
    bool_cols_to_encode = ['gender', 'Partner', 'Dependents', 'PhoneService',  'PaperlessBilling', 'Churn' ]

    # Multi level columns
    multi_level_cols_to_encode = ['MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup', 
                    'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract',
                    'PaperlessBilling', 'PaymentMethod', 'MonthlyCharges_bin', 'TotalCharges_bin', 'tenure_bin']

    cols_to_encode= bool_cols_to_encode + multi_level_cols_to_encode

    # Initialize the encoder
    encoder = OrdinalEncoder()
    # Fit and transform in one go
    df[cols_to_encode] = encoder.fit_transform(df[cols_to_encode]).astype(int)

    df.drop('customerID', axis = 1, inplace = True)

    return df

# Step 5: Define Prefect Flow
@flow(log_prints=True)
def workflow_customer_churn():
    # step 1 = loading data
    data = load_dataset()
    # step 2 = preprocessing
    preprocessed_data = preprocess_data(data)
    print('data is pre-processes')
    print('data shape', data.shape)
    preprocessed_data.to_csv(parent_path / 'data/preprocessed_data.csv', index = False)

# Step 6: Run the Prefect Flowls
if __name__ == "__main__":
    workflow_customer_churn.serve(name="customer_churn-workflow",
                      tags=["first workflow"],
                      parameters={},
                      interval=120)