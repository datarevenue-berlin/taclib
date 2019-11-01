import luigi
from taclib.task import KubernetesTask


class ExampleTask(KubernetesTask):

    name = luigi.Parameter(default="taclib")

    @property
    def image(self):
        return "my-custom-image"

    @property
    def command(self):
        return ["python", "-m", "script.py", "--name", self.name]

    def output(self):
        return luigi.LocalTarget("_SUCCESS")
