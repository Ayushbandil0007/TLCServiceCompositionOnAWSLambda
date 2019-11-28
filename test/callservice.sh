#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"row\"":500,"\"col\"":10,"\"bucketname\"":\"test.bucket.562f19.ayu\"","\"filename\"":\"test.csv\""}

echo "Invoking Lambda function using API Gateway"
time output=`curl -s ­-H "Content-­Type: application/json" -X POST -d $json https://h0npb4op96.execute-api.us-east-1.amazonaws.com/CreateCSV1`
echo ""
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
