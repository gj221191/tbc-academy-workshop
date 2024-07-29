# tbc-academy-workshop

This manual provides step-by-step instructions for setting up a simple blog application using various AWS services.

Table of Contents

Create VPC with Private Subnets
Create Route Table
Create Internet Gateway (IGW)
Create RDS Aurora (MySQL Database)
Create API Gateway
Create Lambda Function (Backend API)
Create SNS Topic
Create SQS Queue
Create DynamoDB Table
Configure IAM Roles and Policies
Set Up Node.js Lambda Environment
Disclaimer

1. Create VPC with Private Subnets
Step-by-Step Instructions:

Go to the VPC Dashboard:

Open the AWS Management Console.
Navigate to the VPC Dashboard.
Create a VPC:

Click on "Create VPC".
Set the name to workshop-vpc.
Set the IPv4 CIDR block to 10.0.0.0/16.
Leave other settings as default.
Click "Create".
Create Subnets:

Click on "Subnets" in the left-hand menu.
Click "Create subnet".
Select workshop-vpc for the VPC.
Create two public subnets:
public-subnet-1a in eu-central-1a with CIDR 10.0.1.0/24.
public-subnet-1b in eu-central-1b with CIDR 10.0.2.0/24.
Create two private subnets:
private-subnet-1a in eu-central-1a with CIDR 10.0.3.0/24.
private-subnet-1b in eu-central-1b with CIDR 10.0.4.0/24.
2. Create Route Table
Step-by-Step Instructions:

Go to the Route Tables:

In the VPC Dashboard, click on "Route Tables".
Create a Route Table:

Click "Create route table".
Name it workshop-route-table.
Select workshop-vpc for the VPC.
Click "Create".
Edit Routes:

Select workshop-route-table.
Click on the "Routes" tab and then "Edit routes".
Add a route:
Destination: 0.0.0.0/0
Target: Select your Internet Gateway (created in the next step).
Click "Save routes".
Associate Subnets:

Go to the "Subnet associations" tab.
Click "Edit subnet associations".
Select public-subnet-1a and public-subnet-1b.
Click "Save associations".
3. Create Internet Gateway (IGW)
Step-by-Step Instructions:

Go to the Internet Gateways:

In the VPC Dashboard, click on "Internet Gateways".
Create an Internet Gateway:

Click "Create internet gateway".
Name it workshop-igw.
Click "Create".
Attach Internet Gateway to VPC:

Select workshop-igw.
Click "Actions" and select "Attach to VPC".
Select workshop-vpc and click "Attach".
4. Create RDS Aurora (MySQL Database)
Step-by-Step Instructions:

Go to the RDS Dashboard:

Open the AWS Management Console.
Navigate to the RDS Dashboard.
Create a DB Subnet Group:

Click on "Subnet groups" in the left-hand menu.
Click "Create DB Subnet Group".
Name it workshop-db-subnet-group.
Select workshop-vpc.
Add the private subnets private-subnet-1a and private-subnet-1b.
Click "Create".
Create an Aurora Database Cluster:

Click "Create database".
Select "Amazon Aurora".
Choose "MySQL-compatible edition".
Select "Serverless" and click "Next".
Set DB cluster identifier to workshop-db-cluster.
Set Master username to admin.
Set Master password and Confirm password.
Under "Network & Security":
Select workshop-vpc.
Choose workshop-db-subnet-group.
Ensure "Publicly Accessible" is set to "No".
Click "Create database".
Note the Database Endpoint:

After the database is created, go to the "Databases" section.
Select your workshop-db-cluster.
Note down the "Writer endpoint" (e.g., workshop-db-cluster.cluster-xyz.us-east-1.rds.amazonaws.com).
5. Create API Gateway
Step-by-Step Instructions:

Go to the API Gateway Dashboard:

Open the AWS Management Console.
Navigate to the API Gateway Dashboard.
Create a REST API:

Click "Create API".
Choose "REST API" and select "Build".
Name it workshop-api.
Click "Create API".
Create Resource and Methods:

Click on "Actions" and select "Create Resource".
Name it v1.
Click "Create Resource".
Create Methods:

For the /v1 resource, create the following methods:
POST /create-table:
Integration type: Lambda Function.
Use the Lambda function created in the next step (API Gateway will call the Lambda function to create the table).
POST /insert:
Integration type: Lambda Function.
Use the Lambda function created in the next step (API Gateway will call the Lambda function to insert a record).
DELETE /delete:
Integration type: Lambda Function.
Use the Lambda function created in the next step (API Gateway will call the Lambda function to delete a record).
Enable CORS:

For each method, click on "Actions" and select "Enable CORS".
Follow the prompts to configure CORS settings.
6. Create Lambda Function (Backend API)
Step-by-Step Instructions:

Go to the Lambda Dashboard:

Open the AWS Management Console.
Navigate to the Lambda Dashboard.
Create a Lambda Function:

Click "Create function".
Select "Author from scratch".
Name it workshop-lambda.
Runtime: Node.js 18.x.
Role: Create a new role with basic Lambda permissions.
Click "Create function".
Configure Lambda Function:

Add the following environment variables:
DB_HOST: The RDS database endpoint.
DB_NAME: workshopdb.
Upload Lambda Code:

Upload the Lambda code from your GitHub repository:
API Gateway Lambda Code
Click on "Upload from" and select "zip file".
Upload the zip file containing the code.
Configure VPC Settings:

Go to the "Configuration" tab.
Under "VPC", configure the VPC settings:
VPC: workshop-vpc.
Subnets: Select the private subnets private-subnet-1a and private-subnet-1b.
Security groups: Create and select a security group that allows access to the RDS database.
7. Create SNS Topic
Step-by-Step Instructions:

Go to the SNS Dashboard:

Open the AWS Management Console.
Navigate to the SNS Dashboard.
Create a Topic:

Click "Create topic".
Choose "Standard".
Name it RecordInsertionTopic.
Click "Create topic".
Subscribe to the Topic:

Click on the topic you just created.
Click "Create subscription".
Protocol: Email.
Endpoint: Enter your email address.
Click "Create subscription".
Confirm the subscription via the email you receive.
8. Create SQS Queue
Step-by-Step Instructions:

Go to the SQS Dashboard:

Open the AWS Management Console.
Navigate to the SQS Dashboard.
Create a Queue:

Click "Create queue".
Name it RecordDeletionQueue.
Set Queue type to "Standard".
Click "Create queue".
Subscribe SQS to Lambda:

Go to the Lambda Dashboard.
Select the process-sqs-messages Lambda function (created in the next step).
Under "Configuration" tab, add an SQS trigger.
Select RecordDeletionQueue.
Click "Add".
9. Create DynamoDB Table
Step-by-Step Instructions:

Go to the DynamoDB Dashboard:

Open the AWS Management Console.
Navigate to the DynamoDB Dashboard.
Create a Table:

Click "Create table".
Table name: RecordDeletionLogs.
Partition key: id (String).
Click "Create table".
10. Configure IAM Roles and Policies
Step-by-Step Instructions:

Create IAM Roles for Lambda Functions:

Go to the IAM Dashboard.
Click on "Roles".
Click "Create role".
Select "Lambda" and click "Next: Permissions".
Attach the following policies:
AWSLambdaBasicExecutionRole
AmazonSQSFullAccess
AmazonSNSFullAccess
AmazonDynamoDBFullAccess
AmazonRDSDataFullAccess
Click "Next: Tags", then "Next: Review".
Name the role workshop-lambda-role.
Click "Create role".
Assign Role to Lambda Functions:

Go to the Lambda Dashboard.
Select each Lambda function (workshop-lambda, process-sqs-messages).
Under "Configuration" tab, click on "Permissions".
Edit the execution role and attach workshop-lambda-role.
11. Set Up Node.js Lambda Environment
Step-by-Step Instructions:

Install Node.js and NPM:

Ensure you have Node.js and npm installed on your machine. You can download and install them from Node.js official website.
Initialize a Node.js Project:

Create a new directory for your Lambda function code.
Navigate to the directory in your terminal.
Run npm init -y to initialize a new Node.js project.
Install Required Packages:

Run the following commands to install the required AWS SDK packages:
sh
Copy code
npm install @aws-sdk/client-secrets-manager
npm install @aws-sdk/client-sns
npm install @aws-sdk/client-sqs
npm install mysql
npm install @aws-sdk/client-dynamodb
Create Lambda Function Files:

Create an index.mjs file in your project directory and paste the following code:
javascript
Copy code
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import mysql from "mysql";

const secret_name = "rds!cluster-b1dd9cd3-b33c-4999-99aa-89b8eb7228eb";
const region = "eu-central-1";
const dbHost = "workshop-db-cluster.cluster-ch4mqacs8qrd.eu-central-1.rds.amazonaws.com";
const dbName = "workshopdb";
const snsTopicArn = "arn:aws:sns:eu-central-1:755440189996:RecordInsertionTopic";
const sqsQueueUrl = "https://sqs.eu-central-1.amazonaws.com/755440189996/RecordDeletionQueue";

const secretsClient = new SecretsManagerClient({ region: region });
const snsClient = new SNSClient({ region: region });
const sqsClient = new SQSClient({ region: region });

let connection;

const getSecret = async () => {
    const data = await secretsClient.send(
        new GetSecretValueCommand({
            SecretId: secret_name,
            VersionStage: "AWSCURRENT",
        })
    );
    return JSON.parse(data.SecretString);
};

const connectToDatabase = async (database) => {
    const secret = await getSecret();
    connection = mysql.createConnection({
        host: dbHost,
        user: secret.username,
        password: secret.password,
        database: database
    });
    connection.connect((err) => {
        if (err) throw err;
    });
};

export const handler = async (event) => {
    const initialDbName = 'mysql';
    await connectToDatabase(initialDbName);

    let method, path;

    if (event.version === "2.0") {
        method = event.requestContext.http.method;
        path = event.rawPath;
    } else {
        method = event.httpMethod;
        path = event.path;
    }

    if (method === "POST" && path === "/create-table") {
        return new Promise((resolve, reject) => {
            const checkDatabaseSQL = `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${dbName}'`;
            const useDatabaseSQL = `USE ${dbName}`;
            const createTableSQL = `
                CREATE TABLE IF NOT EXISTS records (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data VARCHAR(255) NOT NULL
                )
            `;
            const selectRecordsSQL = `SELECT * FROM records`;

            connection.query(checkDatabaseSQL, (error, results) => {
                if (error) {
                    reject({
                        statusCode: 500,
                        body: JSON.stringify({ error: error.message }),
                    });
                } else if (results.length === 0) {
                    const createDatabaseSQL = `CREATE DATABASE ${dbName}`;
                    connection.query(createDatabaseSQL, (error, results) => {
                        if (error) {
                            reject({
                                statusCode: 500,
                                body: JSON.stringify({ error: error.message }),
                            });
                        } else {
                            connection.query(useDatabaseSQL, (error, results) => {
                                if (error) {
                                    reject({
                                        statusCode: 500,
                                        body: JSON.stringify({ error: error.message }),
                                    });
                                } else {
                                    connection.query(createTableSQL, (error, results) => {
                                        if (error) {
                                            reject({
                                                statusCode: 500,
                                                body: JSON.stringify({ error: error.message }),
                                            });
                                        } else {
                                            resolve({
                                                statusCode: 200,
                                                body: JSON.stringify({ message: "Database and table created successfully" }),
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                } else {
                    connection.query(useDatabaseSQL, (error, results) => {
                        if (error) {
                            reject({
                                statusCode: 500,
                                body: JSON.stringify({ error: error.message }),
                            });
                        } else {
                            connection.query(selectRecordsSQL, (error, results) => {
                                if (error) {
                                    reject({
                                        statusCode: 500,
                                        body: JSON.stringify({ error: error.message }),
                                    });
                                } else {
                                    resolve({
                                        statusCode: 200,
                                        body: JSON.stringify({
                                            message: "Database exists",
                                            records: results
                                        }),
                                    });
                                }
                            });
                        }
                    });
                }
            });
        });
    } else if (method === "POST" && path === "/insert") {
        await connectToDatabase(dbName);
        const data = JSON.parse(event.body).data;
        return new Promise((resolve, reject) => {
            const sql = "INSERT INTO records (data) VALUES (?)";
            connection.query(sql, [data], async (error, results) => {
                if (error) {
                    reject({
                        statusCode: 500,
                        body: JSON.stringify({ error: error.message }),
                    });
                } else {
                    // Publish to SNS
                    const snsParams = {
                        Message: `New record inserted with data: ${data}`,
                        TopicArn: snsTopicArn
                    };
                    await snsClient.send(new PublishCommand(snsParams));

                    resolve({
                        statusCode: 200,
                        body: JSON.stringify({ id: results.insertId }),
                    });
                }
            });
        });
    } else if (method === "DELETE" && path === "/delete") {
        await connectToDatabase(dbName);
        const id = event.queryStringParameters.id;
        return new Promise((resolve, reject) => {
            const sql = "DELETE FROM records WHERE id = ?";
            connection.query(sql, [id], async (error, results) => {
                if (error) {
                    reject({
                        statusCode: 500,
                        body: JSON.stringify({ error: error.message }),
                    });
                } else {
                    // Send to SQS
                    const sqsParams = {
                        MessageBody: `Record with ID: ${id} deleted`,
                        QueueUrl: sqsQueueUrl
                    };
                    await sqsClient.send(new SendMessageCommand(sqsParams));

                    resolve({
                        statusCode: 200,
                        body: JSON.stringify({ deleted: results.affectedRows }),
                    });
                }
            });
        });
    } else {
        return {
            statusCode: 400,
            body: JSON.stringify({ error: "Invalid request" }),
        };
    }
};
Package the Lambda Function:

Zip the index.mjs file and the node_modules directory.
Ensure the zip file is structured correctly so that the index.mjs file is at the root level.
Upload Lambda Code to AWS:

Go to the Lambda Dashboard.
Select the workshop-lambda function.
Click on "Upload from" and select "zip file".
Upload the zip file containing the code.
12. Disclaimer
This workshop is for educational purposes. The code and architecture described here are basic implementations meant to illustrate how different AWS services can be integrated. Production environments require more advanced security, scalability, and optimization considerations.

By following these detailed steps, you will be able to set up a simple blog application using various AWS services. Each component and service is configured to work together seamlessly, demonstrating the integration of VPC, RDS, Lambda, API Gateway, SNS, SQS, and DynamoDB.
