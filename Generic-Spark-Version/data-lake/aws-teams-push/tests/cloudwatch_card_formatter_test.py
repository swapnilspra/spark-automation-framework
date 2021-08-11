from src.cloudwatch_card_formatter import *

# "{\"source\": \"s3://zoo/2019-01/02/foo.0.csv\", \"error\": \"failed parsing at bar.py line 11\"}"
MESSAGE_SAMPLE = {NEW_STATE_KEY: "alarm", ALARM_NAME_KEY: "Test alarm name"}

FULL_ENVELOPE = {
  "Records": [
    {
      "EventSource": "aws:sns",
      "EventVersion": "1.0",
      "EventSubscriptionArn": "arn:aws:sns:us-east-1:{{accountId}}:ExampleTopic",
      "Sns": {
        "Type": "Notification",
        "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:ExampleTopic",
        "Subject": "hca-job-failure",
        "Message": json.dumps(MESSAGE_SAMPLE) ,
        "Timestamp": "1970-01-01T00:00:00.000Z",
        "SignatureVersion": "1",
        "Signature": "EXAMPLE",
        "SigningCertUrl": "EXAMPLE",
        "UnsubscribeUrl": "EXAMPLE",
        "MessageAttributes": {
          "Test": {
            "Type": "String",
            "Value": "TestString"
          },
          "TestBinary": {
            "Type": "Binary",
            "Value": "TestBinary"
          }
        }
      }
    }
  ]
}

SNS_SAMPLE = FULL_ENVELOPE['Records'][0]['Sns']

def test_get_sns():
    actual = CardFormatter().get_sns(FULL_ENVELOPE)
    assert actual['MessageId'] == "95df01b4-ee98-5cb9-9903-4c221d41eb5e"

def test_create_alert():
    actual = CardFormatter().create_alert(SNS_SAMPLE, MESSAGE_SAMPLE)
    assert actual['summary'] == '**Cloudwatch Alert**'


def test_create_alert_dismissed():
    actual = CardFormatter().create_alert_dismissed(SNS_SAMPLE, MESSAGE_SAMPLE)
    assert actual['summary'] == '**Cloudwatch Alert Returned to Normal**'
