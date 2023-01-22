import runpy


class RunPy:
    def run_module(self, mod_name, *args, **kwargs):
        runpy.run_module(mod_name=mod_name, *args, **kwargs)

    def run_module(self, path_name, *args, **kwargs):
        runpy.run_path(path_name=path_name, *args, **kwargs)
