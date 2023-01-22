import abc
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Union

from named_pub_sub_manager.brokers.asyncio_requests import multi_async_requests
from named_pub_sub_manager.brokers.email_daemon import EmailProtocols
from named_pub_sub_manager.brokers.runpy import RunPy


@dataclass
class ReceiveStrategy(abc.ABC):
    settings: Dict

    @abc.abstractmethod
    def receive(self, message, *args, **kwargs):
        pass


@dataclass
class HTTP(ReceiveStrategy):
    settings: Union[Dict, List] = field(default_factory=dict)
    message_format: Optional[str] = None

    def receive(
        self,
        message: Union[
            Union[str, int, float],
            Dict[str, Union[str, list, int, float]],
            List[Dict[str, Union[str, list, int, float]]],
        ],
        *args,
        **kwargs,
    ):
        # Send the message via HTTP or HTTPS

        if self.message_format is not None:
            if isinstance(message, dict):
                message = self.message_format.format(**message)
            else:
                message = self.message_format.format(message)

        if isinstance(self.settings, dict):
            self.settings = [self.settings]

        for idx, item in enumerate(self.settings):
            self.settings[idx]["data"] = message

        print(
            "HTTP(ReceiveStrategy).receive.multi_async_requests",
            self.settings,
            self.message_format,
            message,
            *args,
            **kwargs,
        )

        return multi_async_requests(self.settings)

    def __hash__(self):
        return id(self)


@dataclass
class Email(ReceiveStrategy):
    settings: Dict = field(default_factory=dict)
    message_format: Optional[str] = None
    email_protocol: EmailProtocols = None

    def receive(self, message, *args, **kwargs):
        # Send the message via email

        if self.message_format is not None:
            if isinstance(message, dict):
                message = self.message_format.format(**message)
            else:
                message = self.message_format.format(message)

        self.settings["message_format"] = self.message_format

        print(
            "Email(ReceiveStrategy).receive.email_protocol",
            self.settings,
            self.message_format,
            message,
            *args,
            **kwargs,
        )

        email_protocol = self.email_protocol.value(**self.settings)

        if isinstance(email_protocol, EmailProtocols.SMTP.value):

            receiver_email = self.settings.pop("receiver_email")
            subject = self.settings.pop("subject")

            return email_protocol.send_mail(
                receiver_email=receiver_email,
                subject=subject,
                raw_message_text=message,
                *args,
                **kwargs,
            )

        if isinstance(email_protocol, EmailProtocols.POP3.value) or isinstance(
            email_protocol, EmailProtocols.IMAP.value
        ):
            return email_protocol.receive_mail(
                *args,
                **kwargs,
            )

    def __hash__(self):
        return id(self)


@dataclass
class File(ReceiveStrategy):
    settings: Dict = field(default_factory=dict)

    def receive(self, message, *args, **kwargs):
        # Write the message to a file
        print("File", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class SQL(ReceiveStrategy):
    settings: Dict = field(default_factory=dict)

    def receive(self, message, *args, **kwargs):
        # Write the message to a SQL table
        print("SQL", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class SMS(ReceiveStrategy):
    settings: Dict = field(default_factory=dict)

    def receive(self, message, *args, **kwargs):
        # Send the message via SMS
        print("SMS", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class MessageQueue(ReceiveStrategy):
    settings: Dict = field(default_factory=dict)

    def receive(self, message, *args, **kwargs):
        # Enqueue the message in a message queue
        print("MessageQueue", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class ExecutePythonScript(ReceiveStrategy):
    settings: Dict = field(default_factory=dict)

    def receive(self, message, *args, **kwargs):
        # Execute a Python script with the message as an argument

        print(
            "ExecutePythonScript(ReceiveStrategy).receive.RunPy",
            self.settings,
            message,
            *args,
            **kwargs,
        )

        RunPy.run_module(mod_name=message, **self.settings)

        RunPy.run_module(path_name=message, **self.settings)

    def __hash__(self):
        return id(self)


class ReceiveMessageStrategies(Enum):
    HTTP = HTTP
    Email = Email
    File = File
    SQL = SQL
    SMS = SMS
    MessageQueue = MessageQueue
    ExecutePythonScript = ExecutePythonScript


@dataclass
class Subscriber:
    name: str
    receive_strategies: List[ReceiveMessageStrategies]

    def __init__(self, name: str, receive_strategies: List[ReceiveMessageStrategies]):
        self.name: str = name
        self.receive_strategies: List[ReceiveMessageStrategies] = receive_strategies

    def receive(
        self,
        message,
        specific_receive_strategies: Optional[List[ReceiveMessageStrategies]] = None,
        *args,
        **kwargs,
    ):
        intersection_strategies: List[ReceiveMessageStrategies] = []
        if specific_receive_strategies is None:
            intersection_strategies = self.receive_strategies
        else:
            for receive_strategy in self.receive_strategies:
                for strategy in specific_receive_strategies:
                    if isinstance(receive_strategy, strategy):
                        intersection_strategies.append(receive_strategy)

        for strategy in intersection_strategies:
            print(strategy.receive(message, *args, **kwargs))

    def __hash__(self):
        return id(self)


@dataclass
class PubSub:
    subscribers: Set

    def __init__(self, subscribers: Set[Subscriber] = None):
        self.subscribers: Set[Subscriber] = subscribers or set()

    def subscribe(self, subscriber: Subscriber):
        self.subscribers.add(subscriber)

    def unsubscribe(self, subscriber: Subscriber):
        self.subscribers.discard(subscriber)

    def publish(self, message, *args, **kwargs):
        for subscriber in self.subscribers:
            subscriber.receive(message, *args, **kwargs)
            print()
