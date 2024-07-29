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
