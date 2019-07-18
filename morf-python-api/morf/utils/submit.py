# Copyright (c) 2018 The Regents of the University of Michigan
# and the University of Pennsylvania
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Helper functions for job submission to the MORF platform.
"""

import requests
import os

# In case a developer wants to use his own SQS queue,
# the following key can be added to the system environment
SQS_DEBUG_KEY = "MORF_SQS_URL"

SQS_QUEUE_URL = "https://dcd97aapz1.execute-api.us-east-1.amazonaws.com/dev/morf/"

# If the key exists in the system environment, use the value
# of that or the SQS URL

if SQS_DEBUG_KEY in os.environ and os.environ[SQS_DEBUG_KEY] != "":
    SQS_QUEUE_URL = os.environ[SQS_DEBUG_KEY]

MWE_CONFIG_URL = "https://raw.githubusercontent.com/educational-technology-collective/morf/master/mwe/client.config"

def easy_submit(client_config_url, email_to):
    """
    Submit a job to the MORF platform.
    :param client_config_url: URL for config file.
    :param email_to: email address to receive job notifications.
    :param sqs_queue_url: url for submission queue (string).
    :return:
    """
    params = {"url" : client_config_url, "email_to" : email_to}
    r = requests.get(SQS_QUEUE_URL, params=params)
    print(r.text)
    return

def submit_raw_files(container, config, email_to):
    """
    Submit a job to the MORF platform with raw files.
    :param container: raw containerized docker file
    :param email_to: email address to receive job notifications.
    :param config: raw config file
    :return:
    """
    files = {
        "container": container
    }
    params = {
        "config": config.read(),
        "email_to": email_to
    }
    r = requests.post(SQS_QUEUE_URL, files=files, data=params)
    print(r.text)
    return


def submit_mwe(email_to):
    easy_submit(MWE_CONFIG_URL, email_to)
    return
