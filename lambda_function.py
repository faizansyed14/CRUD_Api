import boto3
import json
from custom_encoder import CustomEncoder
import logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)


dynamodbTableName = 'users-data'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)


getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
healthPath = '/health'
userPath = '/user'
usersPath = '/users'



def lambda_handler(event, contest):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
        
    elif httpMethod == getMethod and path == userPath:
        response = getUser(event['queryStringParameters']['emailId'])
        
    elif httpMethod == getMethod and path == usersPath:
        response = getUsers()
        
    elif httpMethod == postMethod and path == usersPath:
        response = saveUser(json.loads(event['body']))
        
    elif httpMethod == patchMethod and path == usersPath:
        requestBody = json.loads(event['body'])
        response = modifyUser(requestBody['emailId'], requestBody['updateKey'], requestBody['updateValue'])
        
    elif httpMethod == deleteMethod and path == usersPath:
        requestBody = json.loads(event['body'])
        response = deleteUser(requestBody['emailId'])
        
    else:
        response = buildResponse(404, 'Not Found')

    return response


def getUser(emailId):
    try:
        response = table.get_item(
            Key={
                'emailId': emailId
            }
        )
        if 'Item' in response:
            return buildResponse(200, response['Item'])
        else:
            return buildResponse(404, {'Message': 'EmailId: %s not found' % emailId})
    except:
        logger.exception('Log it here for now')

def getUsers():
    try:
        response = table.scan()
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])

        body = {
            'users': result
        }
        return buildResponse(200, body)
    except:
        logger.exception('Log it here for now')


def saveUser(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            'Message': 'User Account Created sucessfully!',
            'Item': requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception('Log it here for now')

def modifyUser(emailId, updateKey, updateValue):
    try:
        response = table.update_item(
            Key = {
                'emailId': emailId
            },
            UpdateExpression='set %s = :value' % updateKey,
            ExpressionAttributeValues={
                ':value': updateValue
            },
            ReturnValues='UPDATED_NEW'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'UpdatedAttribute': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Log it here for now')

def delete(emailId
    try:
        response = table.delete_item(
            Key = {
                 'emailId': emailId
            },
            ReturnValues='ALL_OLD'
        )
        body = {
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'deletedItem': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Log it here for now')

def buildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'ContentType': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response
