from pathlib import Path

import luigi
import pytest

from taclib.task import KubernetesTask


@pytest.fixture()
def test_assets():
    return Path(__file__).parent / "assets"


class KubernetesTestTask(KubernetesTask):
    """Testing task runs a drtools/random-task container

    This Task is used mainly to test its parent class
    Kubernetes task by running a drtools/random-task container.
    This will log 10 random messages and then exit succesfully.

    https://github.com/datarevenue-berlin/random-task

    Parameters
    ----------

    out: str
        path to a temporary test directory to write
        success flag to.

    """

    out = luigi.Parameter()
    fail = luigi.BoolParameter(default=False)

    @property
    def image(self):
        return "drtools/random-task"

    @property
    def configuration(self):
        env = ["NLOGS=2"]
        if self.fail:
            env.append("FAIL=true")
        conf = super(KubernetesTestTask, self).configuration
        conf["environment"] = env
        return conf

    @property
    def command(self):
        return ["/run_task"]

    def run(self):
        super().run()
        open(self.output().path, "w").close()

    def output(self):
        return luigi.LocalTarget(self.out)
