from __future__ import print_function
import boto3
import os 
import os.path
import base64
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
import mysql.connector
# import io
# import email
# import re
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="##",
  database="emaildata"
)
bucket_name1='harsh-with-attach'
bucket_name2='harsh-without-attach'
bucket_name3='harsh-withall-attach'

s3 = boto3.client('s3',aws_access_key_id="###3",
                  aws_secret_access_key="###",
                  aws_session_token="##")
mycursor=mydb.cursor()
# mycursor.execute("CREATE TABLE attachments (id INT AUTO_INCREMENT PRIMARY KEY,name varchar(225),binarydata MEDIUMBLOB)")
mydb.commit()
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
date_range = (datetime.utcnow() - timedelta(days=1)).strftime('%Y/%m/%d')
query = f'after:{date_range}'

def main():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('gmail', 'v1', credentials=creds)
        results=service.users().messages().list(userId='me',labelIds=['INBOX'],q="query",maxResults="5").execute()
        messages=results['messages']
        for message in messages:
            messageid=message['id']
            msg =service.users().messages().get(userId='me', id=messageid,).execute()
            payload = msg['payload']
            headers = payload['headers']
            subject = ''
            date = ''
            for header in headers:
                if header['name'] == 'Subject':
                    subject = header['value']
                if header['name'] == 'Date':
                    date = header['value']
            key = f'{subject} - {date}.txt'
            body = msg['snippet']
            print("key:",key)
            print("body:",body)
            if 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                        if part['filename']!="":
                            data2=f"key-{key}\nbody-{body}\nAttachment found-{part['filename']}\n"
                            # import pdb;pdb.set_trace()
                            # s3.put_object(Body=data2.encode('UTF-8'),Bucket=bucket_name1,Key=f"{subject}.txt")
                            attachmentId=part['body']['attachmentId']
                            attachment = service.users().messages().attachments().get(userId='me', messageId=messageid, id=attachmentId).execute()
                            # att_headers=attachment['headers']
                            # print(attachment)
                            data = attachment['data']
                            file_data = base64.urlsafe_b64decode(data.encode('UTF-8')) 
                            # filename = None
                            # for header in att_headers:
                            #     if header['name'].lower() == 'content-disposition':
                            #         match = re.search('filename="(.+)"', header['value'])
                            #         if match:
                            #             filename = match.group(1)
                            # if filename:
                            #     file_ext = os.path.splitext(filename)[1].lower()
                            #     print(file_ext)
                            #     sql="Insert into attachments (name,binarydata) Values (%s, %s)"
                            #     val=(part['filename'],file_data)
                            #     mycursor.execute(sql,val)
                            #     mydb.commit()
                            #     print("successfully data entered")
                            sql="Insert into attachments (name,binarydata) Values (%s, %s)"
                            val=(part['filename'],file_data)
                            mycursor.execute(sql,val)
                            mydb.commit()
                            print("successfully data entered")
                            # with io.BytesIO(file_data) as file: 
                            #     s3.upload_fileobj(file,bucket_name3,part['filename'])
                            
                        else:
                            data=f"key-{key}\nbody-{body}\n"
                            # s3.put_object(Body=data.encode('UTF-8'),Bucket=bucket_name2,Key=f"{subject}.txt")
            print("\n")
    except HttpError as error:
        print(f'An error occurred: {error}')


if __name__ == '__main__':
    main()
