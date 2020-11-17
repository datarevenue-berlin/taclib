"""Task module.

This module provides different task implementations. These implementations
provide standardized ways of running a command in a docker containers and check
for expected output in inputs.

"""
# coding=utf-8
import hashlib
import socket
from logging import getLogger

import luigi

from taclib.config import config
from taclib.container import ContainerClient, K8sClient

class ContainerTask(luigi.Task):
    """Base class to run containers."""

    keep_finished = luigi.BoolParameter(
        default=False,
        description="Don't remove containers " "that finished successfully.",
    )

    CLIENT = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = self.get_client()
        self._container = None
        self.u_name = None
        self._retry_count = 0
        self._log = []

        self.task_log = getLogger("luigi-task-logger")

    def get_client(self) -> ContainerClient:
        """Retrieve execution engine client.

        The client object will be saved to the private attribute `_client`.
        Override this method in case your client needs some special
        initialization.

        Returns
        -------
            client: object
                client to manage containers
        """
        return self.CLIENT()

    def get_logs(self):
        """Return container logs.

        This method returns container logs as a list of lines.
        It will only work if `log_stream` method is implemented.

        It is especially useful if a task does not create a file in a
        filesystem e.g. it might do some database operation in which
        case the log can be written to a file which would also serve
        as a flag that the task finished successfully. For this case
        you will have to override the `run` method to save the logs
        in the end.

        Returns
        -------
            logs: list of str
        """
        return self._log

    @property
    def name(self):
        """We name the resource with luigi's id.

        This id is based on a hash of a tasks parameters and helps avoid running
        the same task twice. If a task with this names already exists and failed
        it will append a 'retry-<NUMBER>' to the name.

        If retry_host_sensitive is set the hostname will be hashed together with the
        luigi task id.
        """
        name_components = self.task_id.split("_")
        name, param_hash = name_components[0], name_components[-1]

        if config["retry_host_sensitive"].get(bool):
            param_hash = param_hash + socket.gethostname()
            param_hash = hashlib.md5(param_hash.encode("utf-8")).hexdigest()[:10]

        if len(name) > 43:
            name = name[:43]

        task_id = "-".join([name, param_hash])
        return task_id.lower().replace("_", "-")

    @property
    def command(self):
        """The command to be executed by the container."""
        raise NotImplementedError("Container task must specify command")

    @property
    def image(self):
        """Which image to use to create the container."""
        raise NotImplementedError("Container tasks must specify image")

    @property
    def labels(self):
        return dict(
            luigi_retries=str(self._retry_count),
            luigi_task_hash=self.task_id.split("_")[-1],
            luigi_host=socket.gethostname(),
            taclib_task_name=self.name,
        )

    @property
    def configuration(self):
        """Container configuration dictionary.

        Should return a dictionary as accepted by docker-py's run method
        see https://docker-py.readthedocs.io/en/stable/containers.html for more
        information which keys are accepted.

        It should be translated into other execution engines format by the
        corresponding subclass.
        """
        return {}

    def _set_name(self):
        if self._retry_count:
            self.u_name = "{}-retry-{}".format(self.name, self._retry_count)
        else:
            self.u_name = self.name

    def run(self):
        """Actually submit and run task as a container."""
        try:
            self._run_and_track_task()
        finally:
            if self._container:
                self._client.stop_container(self._container)
            self._client.remove_succeeded_pods()

    def _run_and_track_task(self):
        self.task_log = getLogger(f"task-log [{self.name}]")

        self.task_log.info(f"Run and track task")
        self._retry_count = self._client.get_retry_count(self.name)
        self._set_name()

        self._container = self._client.run_container(
            self.image, self.u_name, self.command, self.configuration
        )
        self.task_log.info(f"Running task on container {self._container.metadata.name}")

        self._log = []
        log_stream = self._client.log_generator(self._container)

        if log_stream is not None:
            for line in log_stream:
                self._log.append(line.decode().strip())
                getLogger(__name__).info(self._log[-1])
                self.set_status_message("\n".join(self._log))

        exit_info = self._client.get_exit_info(self._container)

        if exit_info[0] == 0:
            if not self.keep_finished:
                self._client.remove_container(self._container)
            self._container = None
        else:
            self.task_log.error(
                "Container exited with status code:"
                " {} and msg: {}".format(exit_info[0], exit_info[1])
            )
            raise RuntimeError(
                "Container exited with status code:"
                " {} and msg: {}".format(exit_info[0], exit_info[1])
            )


class KubernetesTask(ContainerTask):
    """Run tasks as containers on a Kubernetes cluster."""

    CLIENT = K8sClient
    service_account = luigi.OptionalParameter(
        default=None,
        significant=False,
        description="Name of Service Account to be attached to created Pod.",
    )
    node_selector = luigi.OptionalParameter(
        default=None,
        significant=False,
        description="Node selector for this task, use lbl=value syntax. "
        "Combine multiple selector by separating with a comma.",
    )
    k8s_namespace = luigi.OptionalParameter(
        default=config["namespaces"]["default"].get(str),
        significant=config["namespaces"]["significant"].get(bool),
        description="The kubernetes namespace this task should be ran in.",
    )

    def get_client(self) -> ContainerClient:
        return self.CLIENT(namespace=self.k8s_namespace)

    @property
    def image(self):
        return config["image"].get(str)

    @property
    def k8s_resources(self):
        return config["resources"].get()

    @property
    def _node_selector_dict(self):
        res = {}
        if self.node_selector:
            for sel in self.node_selector.split(","):
                lbl, value = sel.split("=")
                lbl = lbl.strip()
                value = value.strip()
                res[lbl] = value
        return res

    @property
    def pod_spec_kwargs(self):
        """See kubernetes.client.models.v1_pod_spec.V1PodSpec
        for possible values.

        If you overwrite this class don't forget to call superclass method in
        case you're using the service_account or node_selector property!
        """
        pod_spec_kwargs = config["pod_spec"].get()
        node_selector = self._node_selector_dict

        if len(node_selector):
            pod_spec_kwargs["node_selector"] = self._node_selector_dict

        if self.service_account:
            pod_spec_kwargs["service_account_name"] = self.service_account

        return pod_spec_kwargs

    @property
    def job_spec_kwargs(self):
        """See kubernetes.client.models.v1_job_spec.V1JobSpec
        for possible values."""
        return config["job_spec"].get()

    @property
    def container_spec_kwargs(self):
        return config["container_spec"].get()

    @property
    def configuration(self):
        default_environment = super().configuration.get("environment", {})
        labels = {"luigi_task_family": self.task_family[:64], "spawned_by": "luigi"}
        labels.update(self.labels)

        # environment and resources these are separate instead of
        # being specified via pod_spec_args because they exist
        # as toplevel arguments in docker run API.
        return {
            "metadata": {"labels": labels},
            "environment": default_environment,
            "resources": self.k8s_resources,
            "pod_spec_kwargs": self.pod_spec_kwargs,
            "job_spec_kwargs": self.job_spec_kwargs,
            "container_spec_kwargs": self.container_spec_kwargs,
        }
