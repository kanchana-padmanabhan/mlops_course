
from prefect import flow, task
from FeatureEngineeringWorkflows import run_feature_engineering
from TrainingWorkflow import run_training
import sys

@flow
def run_training_flow():
    features_df = run_feature_engineering()
    model = run_training(features_df)


if __name__ == "__main__":
    run_training_flow()