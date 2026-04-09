import os
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder

def main():
    import glob
    
    model_path = "model/iforest.joblib"
    
    # Auto-detect any dataset CSV in the data folder
    csv_files = glob.glob("data/*.csv")
    source_files = [f for f in csv_files if "anomalies.csv" not in f]

    if not source_files:
        print("Error: No dataset found in the 'data/' folder.")
        print("Please place your PaySim dataset inside the 'data/' directory.")
        return
        
    data_path = source_files[0]
    print(f"Using dataset file: {data_path}")

    print("Loading data... (this might take a minute)")
    # Load just a sample to train the model to speed it up and avoid memory issues
    df = pd.read_csv(data_path, nrows=200000)
    
    print("Preparing features...")
    numeric_features = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
    categorical_features = ['type']

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ])

    print("Training Isolation Forest... (Anomaly detection model)")
    # We set contamination slightly high just to ensure we catch "anomalies" for the dashboard demo
    model = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', IsolationForest(contamination=0.01, random_state=42, n_jobs=-1))
    ])

    # Fit model on the data
    # Note: we drop 'isFraud' etc. during training as it's unsupervised
    X = df[numeric_features + categorical_features]
    model.fit(X)

    print(f"Saving model to {model_path}...")
    joblib.dump(model, model_path)
    
    print("Model training complete! You are ready to start the pipeline.")

if __name__ == "__main__":
    main()
