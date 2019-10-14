#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `taclib` package."""

from taclib.config import config

import time

import luigi
import os
import pytest

from taclib.container import K8sClient
from taclib.task import KubernetesTask


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
        return 'drtools/random-task'

    @property
    def configuration(self):
        env = ['NLOGS=2']
        if self.fail:
            env.append('FAIL=true')
        conf = super(KubernetesTestTask, self).configuration
        conf['environment'] = env
        return conf

    @property
    def command(self):
        return ['/run_task']

    def run(self):
        super().run()
        open(self.output().path, 'w').close()

    def output(self):
        return luigi.LocalTarget(self.out)


@pytest.mark.skipif(os.environ.get('CI', False) == 'true',
                    reason="No Kubernetes cluster")
def test_k8s_task(tmpdir):
    flag_file = tmpdir.join('oufile.txt')
    task = KubernetesTestTask(out=str(flag_file))
    luigi.build([task], local_scheduler=True, workers=1)

    client = K8sClient()

    # job should have finished successfully so it should have been removed
    # wait a little until kubernetes actually does this
    time.sleep(3)

    assert client.get_executions(task.name) == []


@pytest.mark.skipif(os.environ.get('CI', False) == 'true',
                    reason="No Kubernetes engine")
def test_k8s_task_fail(tmpdir):
    flag_file = tmpdir.join('oufile.txt')
    task = KubernetesTestTask(out=str(flag_file), fail=True)
    task_name = task.name
    client = K8sClient()
    try:
        luigi.build([task], local_scheduler=True, workers=1)

        # check that the job exists as it failed
        first_job = client.get_executions(task_name)[0]
        assert not 'retry' in first_job.metadata.name
        assert first_job.metadata.labels['luigi_retries'] == '0'

        # as the job failed our class should have appended a retry suffix to the
        # job's name. This will be created as a new job in K8s.
        for i in range(3):
            task_retry = KubernetesTestTask(out=str(flag_file), fail=True)

            # Now actually try to run the new retry job.
            luigi.build([task_retry], local_scheduler=True, workers=1)
            assert f'retry-{i+1}' in task_retry.u_name

            # it will fail again so we should still see it in K8s.
            retry_job = client._get_job(task_retry.name)
            assert f'retry-{i+1}' in retry_job.metadata.name
            assert retry_job.metadata.labels['luigi_retries'] == f'{i+1}'
    finally:
        for job in client.get_executions(task_name):
            client.remove_container(job)


def test_container_task_name():
    class MyTaskABC(KubernetesTask):
        pass

    task = MyTaskABC()
    assert 'mytaskabc' in task.name


def test_config():
    assert config["image"].get(str) == "python3:latest"
