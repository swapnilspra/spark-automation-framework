import os
import json

EVENT_MESSAGE_KEY = 'Message'
EVENT_TIMESTAMP_KEY = 'Timestamp'
ALARM_NAME_KEY = 'AlarmName'
OLD_STATE_KEY = 'OldStateValue'
NEW_STATE_KEY = 'NewStateValue'
REASON_KEY = 'NewStateReason'
TRIGGER_KEY = 'Trigger'
METRIC_NAME_KEY = 'MetricName'
LOG_URL_KEY = 'CARD_LOG'

class CardFormatter:
    def create(self, full_event):
        event = self.get_sns(full_event)
        message= json.loads(event[EVENT_MESSAGE_KEY])
        if  message[NEW_STATE_KEY].lower() == 'alarm':
            card = self.create_alert(event,message)
        else:
            card  = self.create_alert_dismissed(event,message)

        return card

    def create_alert_dismissed(self, event,message):
        return { 
            "@type": "MessageCard", 
            "@context": "http://schema.org/extensions", 
            "themeColor": "0076D7",
            "summary": "**New Error**",
            "sections": [ 
                { 
                    "activityTitle": "**" + message[ALARM_NAME_KEY] + " - SOLVED**",
                    "activitySubtitle": "HCA Data Lake issue", 
                    "activityImage": "http://www.sussex.ac.uk/its/images/sl18_green.svg", 
                    "facts": [ 
                        { 
                            "name": "Incident time:", 
                            "value": event[EVENT_TIMESTAMP_KEY] 
                        },
                        { 
                            "name": "Componet:",
                            "value": "AWS Cloudwatch alarm" 
                            
                        },
                        { 
                            "name": "Metric:", 
                            "value":  message[TRIGGER_KEY][METRIC_NAME_KEY]
                            
                        } 
                        ],
                        "markdown": 1 
                    
                } ]
        }

    def create_alert(self, event, message):
        return { 
            "@type": "MessageCard", 
            "@context": "http://schema.org/extensions", 
            "themeColor": "0076D7",
            "summary": "**New Error**",
            "sections": [ 
                { 
                    "activityTitle": "**" + message[ALARM_NAME_KEY] + "**",
                    "activitySubtitle": "HCA Data Lake issue", 
                    "activityImage": "https://www.sccpre.cat/mypng/detail/57-574129_4-warning-stamp-vector-png-transparent-svg-warning.png", 
                    "facts": [ 
                        { 
                            "name": "Description:", 
                            "value": message[REASON_KEY] 
                            
                        },
                        { 
                            "name": "Incident time:", 
                            "value": event[EVENT_TIMESTAMP_KEY] 
                            
                        },
                        { 
                            "name": "Componet:",
                            "value": "AWS Cloudwatch alarm" 
                            
                        },
                        { 
                            "name": "Metric:", 
                            "value":  message[TRIGGER_KEY][METRIC_NAME_KEY]
                            
                        } 
                        ],
                        "markdown": 1 
                    
                } ],
                "potentialAction": [
                    { 
                        "@type": "OpenUri",
                        "name": "View Logs",
                        "targets": [
                            { "os": "default", "uri": os.environ[LOG_URL_KEY] }
                            ]}
                            ] 
            
        }

    def get_sns(self, event_full_envelope):
        return event_full_envelope['Records'][0]['Sns']
