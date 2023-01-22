import os
from dataclasses import dataclass, field
from typing import List

from twilio.rest import Client

from named_pub_sub_manager.brokers.i_sms import I_SMS


@dataclass
class TwilioSMS(I_SMS):
    received_messages: List[str] = field(default_factory=list)

    def __init__(self, account_sid: str, auth_token: str):

        if account_sid is None:
            account_sid = os.environ["TWILIO_ACCOUNT_SID"]

        if auth_token is None:
            auth_token = os.environ["TWILIO_AUTH_TOKEN"]

        self.client: Client = Client(self.account_sid, self.auth_token)

    def send(self, from_, body, to, *args, **kwargs):
        message = self.client.messages.create(
            from_=from_,
            body=body,
            to=to,
            *args,
            **kwargs,
        )

        return message

    def receive(self, message, *args, **kwargs):

        self.received_messages.append(message)

        return message
