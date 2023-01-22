from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class I_SMS(ABC):
    @abstractmethod
    def send(self, *args, **kwargs):
        pass

    @abstractmethod
    def receive(self, *args, **kwargs):
        pass
