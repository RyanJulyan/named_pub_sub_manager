from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Sequence, Union

# Import queues module
from named_pub_sub_manager.services.queue_manager.queues import Queue
from named_pub_sub_manager.services.queue_manager.queues import Stack
from named_pub_sub_manager.services.queue_manager.queues import PriorityQueue
from named_pub_sub_manager.services.queue_manager.queues import Event


class QueueType(Enum):
    QUEUE = "queue"
    STACK = "stack"
    PRIORITY_QUEUE = "priority_queue"


@dataclass
class QueueManager:
    queues: Dict[str, Dict[str, Union[Queue, Stack, PriorityQueue]]] = field(
        default_factory=dict
    )

    def __post_init__(self):
        if self.queues is None or self.queues == {}:
            self.queues = {
                "queue": {"default": Queue()},
                "stack": {"default": Stack()},
                "priority_queue": {"default": PriorityQueue()},
            }

    def enqueue(
        self, queue_type: QueueType, queue_name: str, *elements: Sequence[Event]
    ):
        queue_lookup = {
            "queue": Queue,
            "stack": Stack,
            "priority_queue": PriorityQueue,
        }

        if queue_name in self.queues[queue_type]:
            self.queues[queue_type][queue_name].enqueue(*elements)
        else:
            self.queues[queue_type][queue_name] = queue_lookup[queue_type](*elements)

    def dequeue(
        self,
        queue_type: QueueType,
        queue_name: str,
    ):
        return self.queues[queue_type][queue_name].dequeue()
