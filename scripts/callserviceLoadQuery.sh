#!/bin/bash
# JSON object to pass to Lambda Function

json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.562f19.ank2\"","\"filename\"":\"100_Sales_Records_output.csv\""}

echo "Invoking Lambda function using API Gateway"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name LoadQuery --region us-east-2 --cli-read-timeout 0 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

echo ""
echo "AWS CLI RESULT:"
echo $output | jq
echo ""
