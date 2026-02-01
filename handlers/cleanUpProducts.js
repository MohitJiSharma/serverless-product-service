const { DynamoDBClient, ScanCommand, DeleteItemCommand } = require('@aws-sdk/client-dynamodb');

const {SNSClient,PublishCommand } = require('@aws-sdk/client-sns');

const dynamoDBClient = new DynamoDBClient({ region: process.env.REGION });



const tableName = process.env.DYNAMO_PROD_TABLE;



exports.cleanUpProducts = async (event) => {
    //Find Dynamo DB Items not having Image URL and create 1 hour ago
    //MAKING IT FEW MINUTES
  try {
    const oneHrAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();

    //Create a Scan command to find categories Items
    const scanCmd = new ScanCommand({
        TableName: tableName,
        FilterExpression: "createdAt < :oneHrAgo AND attribute_not_exists(imageUrl)",
        ExpressionAttributeValues: {
            ":oneHrAgo": { S: oneHrAgo }
        }


    });
  

        //Execute the Scan Command:
        const { Items } = await dynamoDBClient.send(scanCmd);

        if (!Items || Items.length === 0) {
            return {
                statusCode: 200,
                body: JSON.stringify({ msg: "No Dynamo Items found" })
            }
        } else {
            let deletedItemCount = 0;
            //Delete Dynamo Items
            for(const item of Items){

                const deleteCmd = new DeleteItemCommand({
                    TableName: tableName,
                    Key:{id:{S:item.id.S}}
                })

                const response = await dynamoDBClient.send(deleteCmd);
                deletedItemCount++;
            }
            const snsClient = new SNSClient({region: process.env.REGION});

            const snsTopicARN = process.env.SNS_TOPIC_ARN;
            const publishCommand = new PublishCommand({
                TopicArn: snsTopicARN,
                Message: "Records deleted successfully",
                Subject: "AWS Deletion notification"

            });

            await snsClient.send(publishCommand);

            return{
                statusCode:200,
                body: JSON.stringify({msg:`${deletedItemCount} items deleted successfully`})
            }
        }


    } catch (error) {
        return {
            statusCode: 500,
            body: JSON.stringify({ msg: error.message })
        }
    }



};

