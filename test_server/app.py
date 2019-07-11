from flask import Flask, request
from flask import render_template
from werkzeug.utils import secure_filename
from flask_cors import CORS
import boto3
import os
import datetime
import pandas as pd
from morf.utils.alerts import send_queueing_alert
import logging
import traceback
logger = logging.getLogger('morf')

app = Flask(__name__)
CORS(app)

ACCESS_KEY = os.environ["AWS_ETC_ACCESS_KEY"]
SECRET_KEY = os.environ["AWS_ETC_SECRET_KEY"]
BUCKET_NAME = os.environ["AWS_ETC_MORF_BUCKET"]
S3_LOCATION = "http://{}.s3.amazonaws.com/".format(BUCKET_NAME)
AWS_REGION = "us-east-1"

#todo: fetch these from server.config instead
queue_url = "https://sqs.us-east-1.amazonaws.com/010192764715/SagnikMORFTest"

def uploadFileToBucket(s3, file, acl="public-read"):

    try:
        s3.upload_fileobj(
            file,
            BUCKET_NAME,
            file.filename,
            ExtraArgs = {
                "ACL": acl,
                "ContentType": file.content_type
            }
        )
    except Exception as e:
        logger.exception("There was an error while uploading")
        return e

    return "{}{}".format(S3_LOCATION, file.filename)

@app.route("/")
def index():
    return "There was an error", 404


@app.route("/morf/", methods=["GET", "POST"])
def morf():
    # Create SQS client
    sqs = boto3.client("sqs", region_name=AWS_REGION, aws_access_key_id=ACCESS_KEY, 
        aws_secret_access_key=SECRET_KEY)

    if request.method == 'POST':
        
        s3 = boto3.client("s3", region_name=AWS_REGION, aws_access_key_id=ACCESS_KEY, 
        aws_secret_access_key=SECRET_KEY)

        config_file = request.files["config"]
        config_file.filename = secure_filename(config_file.filename)
        docker_file = request.files["container"]
        docker_file.filename = secure_filename(docker_file.filename)

        docker_url = uploadFileToBucket(s3, docker_file)
        config_url = uploadFileToBucket(s3, config_file)

        print("Container uploaded to ", docker_url)

        print("[INFO] received config_url {}".format(config_url))
        logger.info("user from IP {} submitted {} which was uploaded to S3".format(request.remote_addr, config_url))

    else:

        config_url = request.args["url"]
        print("[INFO] received config_url {}".format(config_url))
        logger.info("user from IP {} submitted {}".format(request.remote_addr, config_url))

    try:
        email_to = request.args["email_to"]
        print("[INFO] received email_to {}".format(email_to))
    except KeyError:
        email_to = ""

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=0,
        MessageAttributes={
            "Timestamp": {
                "DataType": "String",
                "StringValue": str(datetime.datetime.now())
            },
            "clientConfigUrl": {
                "DataType": "String",
                "StringValue": str(config_url)
            },
            "emailTo": {
                "DataType": "String",
                "StringValue": str(email_to)
            }
        },
        MessageBody=(
            "Client job submission for MORF."
        )
    )

    print(response["MessageId"])
    print("[INFO] Job queued for execution config {}".format(config_url))
    return "Job submitted to queue"

@app.errorhandler(404)
def page_not_found(e):
    return "There was an error", 404

if __name__ == "__main__":
    app.run()