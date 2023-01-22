import abc
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Union

from named_pub_sub_manager.brokers.asyncio_requests import multi_async_requests
from named_pub_sub_manager.brokers.email_daemon import EmailProtocols
from named_pub_sub_manager.brokers.i_sms import I_SMS
from named_pub_sub_manager.brokers.runpy import RunPy


@dataclass
class ProcessStrategy(abc.ABC):
    settings: Dict

    @abc.abstractmethod
    def process(self, message, *args, **kwargs):
        pass


@dataclass
class HTTP(ProcessStrategy):
    settings: Union[Dict, List] = field(default_factory=dict)
    message_format: Optional[str] = None

    def process(
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
            "HTTP(ProcessStrategy).process.multi_async_requests",
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
class Email(ProcessStrategy):
    settings: Dict = field(default_factory=dict)
    message_format: Optional[str] = None
    email_protocol: EmailProtocols = None

    def process(self, message, *args, **kwargs):
        # Send the message via email

        if self.message_format is not None:
            if isinstance(message, dict):
                message = self.message_format.format(**message)
            else:
                message = self.message_format.format(message)

        self.settings["message_format"] = self.message_format

        print(
            "Email(ProcessStrategy).process.email_protocol",
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
class File(ProcessStrategy):
    settings: Dict = field(default_factory=dict)

    def process(self, message, *args, **kwargs):
        # Write the message to a file
        print("File", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class SQL(ProcessStrategy):
    settings: Dict = field(default_factory=dict)

    def process(self, message, *args, **kwargs):
        # Write the message to a SQL table
        print("SQL", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class SMS(ProcessStrategy):
    settings: Dict = field(default_factory=dict)
    sms_broker: I_SMS = None

    def process(self, message, *args, **kwargs):
        # Send the message via SMS
        print("SMS", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class MessageQueue(ProcessStrategy):
    settings: Dict = field(default_factory=dict)

    def process(self, message, *args, **kwargs):
        # Enqueue the message in a message queue
        print("MessageQueue", self.settings, message, *args, **kwargs)

    def __hash__(self):
        return id(self)


@dataclass
class ExecutePythonScript(ProcessStrategy):
    settings: Dict = field(default_factory=dict)

    def process(self, message, *args, **kwargs):
        # Execute a Python script with the message as an argument

        print(
            "ExecutePythonScript(ProcessStrategy).process.RunPy",
            self.settings,
            message,
            *args,
            **kwargs,
        )

        run_type = self.settings["run_type"]

        RunPy.functions[run_type](message, **self.settings)

    def __hash__(self):
        return id(self)


class ProcessMessageStrategies(Enum):
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
    process_strategies: List[ProcessMessageStrategies]

    def __init__(self, name: str, process_strategies: List[ProcessMessageStrategies]):
        self.name: str = name
        self.process_strategies: List[ProcessMessageStrategies] = process_strategies

    def process(
        self,
        message,
        specific_process_strategies: Optional[List[ProcessMessageStrategies]] = None,
        *args,
        **kwargs,
    ):
        intersection_strategies: List[ProcessMessageStrategies] = []
        if specific_process_strategies is None:
            intersection_strategies = self.process_strategies
        else:
            for process_strategy in self.process_strategies:
                for strategy in specific_process_strategies:
                    if isinstance(process_strategy, strategy):
                        intersection_strategies.append(process_strategy)

        for strategy in intersection_strategies:
            print(strategy.process(message, *args, **kwargs))

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
            subscriber.process(message, *args, **kwargs)
            print()
