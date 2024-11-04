import boto3 
import datetime
import os
from datetime import datetime, timedelta, timezone

USERPOOL_ID = os.environ.get('USERPOOL_ID')


# Initialize Cognito client
cognito_client = boto3.client('cognito-idp')


def list_all_users():
    response = cognito_client.list_users(
        UserPoolId=USERPOOL_ID,
        Limit=60
    )
    
    users = response['Users']
    
    while 'PaginationToken' in response:
        response = cognito_client.list_users(
            UserPoolId=USERPOOL_ID,
            PaginationToken=response['PaginationToken'],
            Limit=60
        )
        users += response['Users']
    
    return users
        
def lambda_handler(event, context):
    users = list_all_users()
            
    # Iterate through each user and check their last login date
    for user in users:
        username = user['Username']
        
        admin_user=cognito_client.admin_get_user(
            UserPoolId=USERPOOL_ID,
            Username=username,
        )
        # filter out users automatically created by external provider
        if admin_user["UserStatus"] != 'EXTERNAL_PROVIDER':
            print(admin_user['Username'])
            username=admin_user['Username']
            creation_date = admin_user['UserCreateDate']

            # Get the user's authentication events
            admin_list = cognito_client.admin_list_user_auth_events(
                UserPoolId=USERPOOL_ID,
                Username=username,
                MaxResults=1
            )
        
            # Get the most recent sign-in event
            auth_events = admin_list['AuthEvents']
            last_signin_event = None
            for event in auth_events:
                if event['EventType'] == 'SignIn':
                    last_signin_event = event
                    break
            
            if last_signin_event is not None:
                last_signin_date = last_signin_event['CreationDate']
                current_time = datetime.now(timezone.utc)
                time_difference = current_time - last_signin_date

            
                # If the user hasn't signed in for 90 days, disable the user
                if time_difference > timedelta(days=90):
                    # Disable the user
                    cognito_client.admin_disable_user(
                        UserPoolId=USERPOOL_ID,
                        Username=username
                    )

            if last_signin_event is None:
                current_time = datetime.now(timezone.utc)
                time_difference = current_time - creation_date

                
                # If the user has been created 90 days, and has no Signin event disable the user
                if time_difference > timedelta(days=90):
                    # Disable the user
                    cognito_client.admin_disable_user(
                        UserPoolId=USERPOOL_ID,
                        Username=username
                    )
                    print(f"User {username} has been disabled.")

    return {
        'statusCode': 200,
        'body': 'Inactive Cognito users deactivated successfully.'
    }
