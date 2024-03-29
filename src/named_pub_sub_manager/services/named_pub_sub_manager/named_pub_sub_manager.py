from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Set

# Import pub_sub module
from named_pub_sub_manager.services.named_pub_sub_manager.pub_sub import PubSub
from named_pub_sub_manager.services.named_pub_sub_manager.pub_sub import Subscriber
from named_pub_sub_manager.services.named_pub_sub_manager.pub_sub import (
    ProcessMessageStrategies,
)


@dataclass
class NamedPubSubManager:
    named_pub_sub: Dict[str, PubSub] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.named_pub_sub is None or self.named_pub_sub == {}:
            self.named_pub_sub = {
                "default": PubSub(),
            }

    def add_named_pub_sub(
        self, pub_sub_name: str, subscribers: Set[Subscriber] = set()
    ) -> None:
        if pub_sub_name not in self.named_pub_sub:
            self.named_pub_sub[pub_sub_name] = PubSub(subscribers=subscribers)
            print(f"Added named_pub_sub: {pub_sub_name}")
            print()

    def subscribe(self, pub_sub_name: str, subscriber: Subscriber) -> None:
        self.create_pub_sub_name_if_not_exists(pub_sub_name)
        self.named_pub_sub[pub_sub_name].subscribe(subscriber)
        print(f"Subscribed: {subscriber.name} to {pub_sub_name}")
        print()

    def unsubscribe(self, pub_sub_name: str, subscriber: Subscriber) -> None:
        self.create_pub_sub_name_if_not_exists(pub_sub_name)
        self.named_pub_sub[pub_sub_name].unsubscribe(subscriber)
        print(f"Unsubscribed: {subscriber.name} from {pub_sub_name}")
        print()

    def publish(self, pub_sub_name: str, message, *args: Any, **kwargs: Any) -> None:
        self.create_pub_sub_name_if_not_exists(pub_sub_name)
        self.named_pub_sub[pub_sub_name].publish(message, *args, **kwargs)
        print(f"Published: {message} to {pub_sub_name}")
        print()
        print("=======================================")

    def create_pub_sub_name_if_not_exists(self, pub_sub_name: str):
        named_pub_sub: PubSub = self.named_pub_sub.get(pub_sub_name)
        if named_pub_sub is None:
            self.add_named_pub_sub(pub_sub_name=pub_sub_name)
