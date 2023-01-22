from dataclasses import dataclass
from typing import ClassVar
import runpy


@dataclass
class RunPy:
    functions: ClassVar

    def __post_init__(self):
        self.functions = {
            "run_module": self.run_module,
            "run_path": self.run_path,
        }

    def run_module(self, mod_name, *args, **kwargs):
        runpy.run_module(mod_name=mod_name, *args, **kwargs)

    def run_path(self, path_name, *args, **kwargs):
        runpy.run_path(path_name=path_name, *args, **kwargs)
