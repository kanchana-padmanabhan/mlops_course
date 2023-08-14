
from flytekit import Resources, task, workflow
from FeatureEngineeringWorkflows import run_feature_engineering
from TrainingWorkflow import run_training
import sys

@workflow
def run_training_flow():
    features_df = run_feature_engineering()
    model = run_training(features_df=features_df)
