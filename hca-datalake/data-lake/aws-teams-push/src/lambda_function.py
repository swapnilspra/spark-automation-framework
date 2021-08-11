import os
from importlib import import_module
from env_url_loader import EnvUrlLoader
from teams_sender import TeamsSender 

ENV_CARD_NAME = 'CARD_BUILDER'

def load_card_formatter(module_name):
    print(f'Using {module_name}')
    return import_module(module_name).CardFormatter()

def send_notification(event, context):
    card_formatter = load_card_formatter(os.environ[ENV_CARD_NAME])
    sender = TeamsSender( EnvUrlLoader(), card_formatter)
    
    sender.send(event,context)

