import json
import pickle
import socket
from collections import OrderedDict
from unittest import mock
from unittest.mock import MagicMock

import pytest

from taclib.container import K8sClient
from .conftest import KubernetesTestTask


def mock_client(job, task, retries=0):
    task._client.run_container = MagicMock(
        autospec=K8sClient.run_container, return_value=job
    )
    task._client.log_generator = MagicMock(
        autospec=K8sClient.log_generator,
        return_value=(f"line_{i}".encode() for i in range(20)),
    )
    task._client.get_exit_info = MagicMock(
        autospec=K8sClient.get_exit_info, return_value=(0, "All G!")
    )
    task._client.get_retry_count = MagicMock(
        autospec=K8sClient.get_retry_count, return_value=retries
    )


@pytest.fixture()
def job(test_assets):
    with (test_assets / "job.pkl").open("rb") as fp:
        return pickle.load(fp)


@pytest.fixture()
def failed_jobs(test_assets):
    return "Jobs"


@mock.patch("taclib.task.KubernetesTask.CLIENT", autospec=K8sClient)
def test_k8s_task_name(mock):
    task = KubernetesTestTask(out="/tmp/test")
    assert "kubernetestesttask" in task.name


@mock.patch("taclib.task.KubernetesTask.CLIENT", autospec=K8sClient)
def test_k8s_task_run(mock, job):
    task = KubernetesTestTask(out="/tmp/test")
    mock_client(job, task)

    # luigi sets this dynamically so we need to mock it
    task.set_status_message = MagicMock()

    task.run()
    assert len(task._log) == 20
    assert task.set_status_message.call_count == 20
    assert "retry" not in task.u_name


@mock.patch("taclib.task.KubernetesTask.CLIENT", autospec=K8sClient)
def test_k8s_task_retry_run(mock, job):
    task = KubernetesTestTask(out="/tmp/test")
    mock_client(job, task, 2)

    # luigi sets this dynamically so we need to mock it
    task.set_status_message = MagicMock()

    task.run()
    assert len(task._log) == 20
    assert task.set_status_message.call_count == 20
    assert "retry" in task.u_name


@mock.patch("taclib.task.KubernetesTask.CLIENT", autospec=K8sClient)
def test_task_node_selector(mock):
    task = KubernetesTestTask(out="/tmp/test", node_selector="memory=huge")
    assert task.pod_spec_kwargs["node_selector"] == {"memory": "huge"}

    task = KubernetesTestTask(out="/tmp/test", node_selector="memory=huge,lbl=label_1")
    assert task.pod_spec_kwargs["node_selector"] == {"memory": "huge", "lbl": "label_1"}


@mock.patch("taclib.task.KubernetesTask.CLIENT", autospec=K8sClient)
def test_task_namespace(mock):
    task = KubernetesTestTask(out="/tmp/test", k8s_namespace="production")
    assert task.k8s_namespace == "production"
    task.CLIENT.assert_called_once_with(namespace="production")


@mock.patch("taclib.task.KubernetesTask.CLIENT", autospec=K8sClient)
def test_task_configuration(mock):
    expected = {
        "metadata": {
            "labels": {
                "luigi_task_family": "KubernetesTestTask",
                "spawned_by": "luigi",
                "luigi_retries": "0",
                "luigi_task_hash": "a9b597ad81",
                "luigi_host": f"{socket.gethostname()}",
            }
        },
        "environment": ["NLOGS=2"],
        "resources": OrderedDict(),
        "pod_metadata": {"annotations": {"safe-to-evict": "true"}},
        "pod_spec_kwargs": OrderedDict(
            [("node_selector", {"memory": "huge"}), ("service_account_name", "task-sa")]
        ),
        "job_spec_kwargs": OrderedDict(),
        "container_spec_kwargs": OrderedDict([("image_pull_policy", "IfNotPresent")]),
    }

    task = KubernetesTestTask(
        out="/tmp/test", node_selector="memory=huge", service_account="task-sa"
    )

    conf = task.configuration
    assert (
        conf["metadata"]["labels"]
        .pop("taclib_task_name")
        .startswith("kubernetestesttask")
    )
    assert json.dumps(conf, sort_keys=True) == json.dumps(expected, sort_keys=True)
