import { SQSClient, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { v4 as uuidv4 } from 'uuid';

const region = "eu-central-1";
const sqsQueueUrl = "https://sqs.eu-central-1.amazonaws.com/755440189996/RecordDeletionQueue";
const dynamoDBTableName = "RecordDeletionLogs";

const sqsClient = new SQSClient({ region: region });
const dynamoDBClient = new DynamoDBClient({ region: region });

export const handler = async (event) => {
    console.log("SQS event received:", JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        const message = record.body;
        console.log("Processing message:", message);

        // Generate a unique ID for the log
        const logId = uuidv4();

        // Store the deletion log in DynamoDB
        const putItemParams = {
            TableName: dynamoDBTableName,
            Item: {
                id: { S: logId },
                message: { S: message },
                timestamp: { S: new Date().toISOString() }
            }
        };

        try {
            await dynamoDBClient.send(new PutItemCommand(putItemParams));
            console.log("Successfully logged the deletion in DynamoDB");
        } catch (error) {
            console.error("Error logging the deletion in DynamoDB:", error);
        }

        // Delete the message from the queue
        const deleteParams = {
            QueueUrl: sqsQueueUrl,
            ReceiptHandle: record.receiptHandle
        };

        try {
            await sqsClient.send(new DeleteMessageCommand(deleteParams));
            console.log("Successfully deleted the message from the queue");
        } catch (error) {
            console.error("Error deleting the message from the queue:", error);
        }
    }
};