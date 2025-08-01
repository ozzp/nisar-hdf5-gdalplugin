#!/bin/bash

# This script retrieves AWS credentials and region from a specified profile
# and outputs them as name-value pairs suitable for environment variables.

# Check if a profile name was provided as a command-line argument.
if [ -z "$1" ]; then
    echo "Usage: $0 <aws-profile-name>"
    exit 1
fi

# Assign the first command-line argument to the PROFILE variable.
PROFILE="$1"

# Fetch configuration values from the specified AWS profile.
AWS_REGION=$(aws --profile "${PROFILE}" configure get region)
AWS_ACCESS_KEY_ID=$(aws --profile "${PROFILE}" configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws --profile "${PROFILE}" configure get aws_secret_access_key)
AWS_SESSION_TOKEN=$(aws --profile "${PROFILE}" configure get aws_session_token)

# Output the name-value pairs to standard out.
echo "export AWS_REGION=${AWS_REGION}"
echo "export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
echo "export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
echo "export AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}"
