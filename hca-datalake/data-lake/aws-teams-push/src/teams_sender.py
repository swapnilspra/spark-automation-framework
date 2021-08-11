import json
import logging
import os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

class TeamsSender:
    def __init__(self, url_loader, card_formatter):
        self.url_loader = url_loader
        self.card_formatter = card_formatter
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def send(self, event, context):
        card = self.card_formatter.create(event)
        channel_urls = self.url_loader.load()

        for url in channel_urls:
            req = Request(url, json.dumps(card).encode('utf-8'))

            try:
                response = urlopen(req)
                response.read()
                self.logger.info(f"Sent to {url}")
            except HTTPError as e:
                self.logger.error(f"Request failed with HTTP Error {e.code}: {e.reason}")
            except URLError as e:
                self.logger.error(f"Server connection failed: {e.reason}")
            
