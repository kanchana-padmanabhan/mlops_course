cd "/Users/kanchanapadmanabhan/OneDrive/Personal-Course/Vector/Model Deployment/mlops_course/Week 1/inference/batch_job"

source ../../env_variables.sh

# Get the current date in YYYY-MM-DD format
current_date=$(date +%Y-%m-%d)

# Convert current date to seconds since the Unix epoch
current_timestamp=$(date -j -f "%Y-%m-%d" "$current_date" "+%s")

# Calculate timestamps for start date (current date plus one day)
start_timestamp=$((current_timestamp + 86400))  # Adding 86400 seconds for one day

# Calculate timestamps for end date (start date plus three months)
end_timestamp=$((start_timestamp + 7776000))   # Adding 7776000 seconds for three months

# Convert timestamps back to date format
export start_date=$(date -r "$start_timestamp" "+%Y-%m-%d")
export end_date=$(date -r "$end_timestamp" "+%Y-%m-%d")
source ../../venv/bin/activate
python BatchScoring.py
deactivate