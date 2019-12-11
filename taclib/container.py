import socket
from logging import getLogger

from kubernetes.config import ConfigException
from kubernetes.watch import Watch
from urllib3.exceptions import ReadTimeoutError

from taclib.config import config

try:
    import docker
    from docker.errors import NotFound

    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False
    pass
try:
    from kubernetes import client, watch
    import kubernetes.config as k8s_config
    from kubernetes.client.rest import ApiException

    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False
    pass


class ContainerNotFound(Exception):
    pass


class ContainerClient:
    def run_task(self, image, name, command, configuration):
        """Method used to submit/run a container.

        This method should return a reference to the contianer.

        Parameters
        ----------
        image: str
            image name to run
        name: str
            The name for this container
        command: str or list
            The command to run in the container.
        configuration: dict
            configuration like accepted by docker-py's run method see
            https://docker-py.readthedocs.io/en/stable/containers.html
            your client implementation might need to translate this into
            kwargs accepted by your execution engine in this method.
        Returns
        -------
            container: obj
                container reference
        """
        raise NotImplementedError()

    def log_generator(self, container):
        """Generator to log stream.

        This method can return a generator to the containers log stream.
        If implemented the container logs will show up in the central scheduler
        as well as in the controller log. Generator is supposed to return lines
        as bytes.

        Parameters
        ----------
        container: contianer reference
            A container reference as returned from run_container method

        Returns
        -------
            log_stream: generator[bytes]
        """
        return None

    def get_exit_info(self, container):
        """Retrieve task exit status

        This method should return a tuple like (exit_code, exit_message). In
        case the container is still running it should block until exit
        information is available.

        Parameters
        ----------
        container: container reference
            A container reference as returned from run_container method

        Returns
        -------
            exit_info: tuple
        """
        raise NotImplementedError()

    def remove_task(self, container):
        """Remove a container from the execution engine.

        This method should remove the container and will be called
        only if the container finished successfully.

        Parameters
        ----------
        container: contianer reference
            A container reference as returned from run_container method

        Returns
        -------
            None
        """
        raise NotImplementedError()

    def stop_task(self, container):
        """Method to stop a running container

        Parameters
        ----------
        container: contianer reference
            A container reference as returned from run_container method

        Returns
        -------
            None
        """
        raise NotImplementedError()

    def get_task(self, u_name):
        """Retrieve container reference from name.

        Parameters
        ----------
        u_name: str
            unique container name or id usually contains retry suffix.

        Returns
        -------
            container: obj
                container reference
        """
        raise NotImplementedError()

    def get_executions(self, task_id):
        """Retrieve all previous runs of this task.

        The return value should be a list of container or job instances. It
        should be ordered by ascending retry count such that the last object
        is the most recently run.

        Parameters
        ----------
        task_id: str
            unique task name usually based on parameter hash.

        Returns
        -------
            list: list[container]
                list of container or job objects
        """
        raise NotImplementedError()

    def get_retry_count(self, task_id):
        """Get number of retries of this task from container engine.

        If last execution is still running it should return it's number else
        it should return the number of failed executions + 1

        Parameters
        ----------
        task_id: str
            unique task name usually based on parameter hash.

        Returns
        -------
            retry_count: int
        """
        raise NotImplementedError()


class K8sClient(ContainerClient):
    """A kubernetes client to run containerized tasks.

    Containers are ran via the Kubernetes batch/V1.Job resource.

    This uses the official kubernetes python client. In case your Job already
    exists it is retrieved instead of newly creating it.
    """

    def __init__(self, namespace="default"):
        if not KUBERNETES_AVAILABLE:
            raise ImportError("Kubernetes python package is not available!")
        try:
            k8s_config.load_incluster_config()
        except ConfigException:
            k8s_config.load_kube_config()

        self._c = client.CoreV1Api()
        self._c_batch = client.BatchV1Api()
        self.namespace = namespace

    @staticmethod
    def _docker_env_to_k8s_env(env):
        """Translate docker environment vars format to kubernetes format."""
        res = []
        if isinstance(env, dict):
            for key, value in env.items():
                res.append(client.V1EnvVar(name=key, value=value))
        elif isinstance(env, (list, tuple)):
            for el in env:
                key, value = el.split("=")
                res.append(client.V1EnvVar(name=key, value=value))
        return res

    @staticmethod
    def _make_job_spec(
        name, image, cmd, resources, env, metadata, pod_spec_kwargs, job_spec_kwargs
    ):
        """Create job specification."""
        spec = client.V1JobSpec(
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="task",
                            image=image,
                            command=cmd,
                            env=K8sClient._docker_env_to_k8s_env(env),
                            image_pull_policy="Always",
                            resources=client.V1ResourceRequirements(**resources),
                        )
                    ],
                    restart_policy="Never",
                    image_pull_secrets=None,
                    **pod_spec_kwargs,
                )
            ),
            **job_spec_kwargs,
        )
        metadata = metadata.copy()
        metadata["name"] = name
        metadata = client.V1ObjectMeta(**metadata)
        return client.V1Job(api_version="batch/v1", spec=spec, metadata=metadata)

    def run_container(self, image, name, command, configuration):
        """Run a container job in a kubernetes pod.

        The job is managed via a  batch/V1.Job resource.

        Parameters
        ----------
        image: str
            the name of the image including tag
        name: str
            metadata.name attribute of the job. Maximum 64 characters.
        command: list[str]
            the command which should be run in the container.
        configuration: dict
            further configuration for the job specification. Expected keys
            are 'metadata', 'environment' and 'resources'. The metadata should
            be a dictionary as described in goo.gl/ngMEX2. The 'environment' key
            accepts environment variables as list of strings with name and value
            separated by '=', or as a dictionary. Finally resources is a
            dictionary which accepts resources and resource limits
            as described in goo.gl/kXU2dK.
            Additionally, 'pod_spec_kwargs' may be specified as well as
            `job_spec_kwargs`. This will be passed to the respective pod and
            job specifications. See https://bit.ly/2J2zPb3 and
            https://bit.ly/2LpTFPe for more details on allowed keys and types.

        Returns
        -------
            job: V1Job
        """
        log = getLogger(__name__)
        body = self._make_job_spec(
            name,
            image,
            command,
            configuration.get("resources"),
            configuration.get("environment"),
            configuration.get("metadata"),
            pod_spec_kwargs=configuration.get("pod_spec_kwargs"),
            job_spec_kwargs=configuration.get("job_spec_kwargs"),
        )
        try:
            job = self._c_batch.create_namespaced_job(self.namespace, body)
            self._wait_for_status(
                job, "Running", timeout=config["init_timeout"].get(int)
            )
        except ApiException as e:
            if e.status == 409:
                # job already exists
                log.warning("Received API exception 409.")
                job = self._get_job(
                    configuration["metadata"]["labels"]["taclib_task_name"]
                )
                log.info("Found existing job for this task. " "Will try to reconnect")
                # KubernetesTask will receive a job object and depending on
                # its status it will either reconnect to the log stream or
                # raise an error end retry the task (using luigi's retry logic)
            elif e.status == 400:
                log.error(str(body))
                raise e
            else:
                raise e
        return job

    def get_executions(self, task_id, include_hostname=None):
        """Get a job resource by its name."""
        if include_hostname or config["retry_host_sensitive"].get(bool):
            lbl_sel = f"taclib_task_name={task_id},luigi_host={socket.gethostname()}"
        else:
            lbl_sel = f"taclib_task_name={task_id}"
        res = self._c_batch.list_namespaced_job(self.namespace, label_selector=lbl_sel)
        jobs = sorted(res.items, key=lambda x: x.metadata.labels["luigi_retries"])
        return jobs

    def _get_job(self, task_id):
        try:
            jobs = self.get_executions(task_id)
            job = jobs[-1]
        except IndexError:
            raise ContainerNotFound("Could not find existing job")
        return job

    def _wait_for_status(self, job, desired_status, timeout=15):
        """Wait for a certain status."""
        pod = self._get_pod(job)
        w = watch.Watch()
        generator = w.stream(
            self._c.list_namespaced_pod,
            namespace=self.namespace,
            field_selector=f"metadata.name={pod.metadata.name}",
            _request_timeout=timeout,
        )

        status = self._get_pod(job).status.phase
        if status == desired_status:
            return True
        if status == "Failed":
            return False

        try:
            for event in generator:
                if event["type"] == "MODIFIED":
                    status = event["object"].status.phase
                    if status == desired_status:
                        break
                    elif status == "Failed":
                        return False
            return True
        except ReadTimeoutError:
            log = getLogger(__name__)
            log.warning(
                "Timeout while waiting for status %s! Pod had status:"
                " %s" % (desired_status, self._get_pod(job).status.phase)
            )
            return False

    def log_generator(self, container):
        pod = self._get_pod(container)
        log = getLogger(__name__)

        log.info(f"Waiting for job {container.metadata.name} to run...")
        self._wait_for_status(container, "Running")
        w = Watch()
        for e in w.stream(
            self._c.read_namespaced_pod_log,
            name=pod.metadata.name,
            namespace=self.namespace,
        ):
            yield e.encode()

    def _get_pod(self, job, timeout=15):
        """Get pod spawned by a certain job."""
        job_name = job.metadata.name
        w = watch.Watch()
        generator = w.stream(
            self._c.list_namespaced_pod,
            self.namespace,
            label_selector=f"job-name={job_name}",
            _request_timeout=timeout,
        )

        try:
            for event in generator:
                pod = event["object"]
                break
            return pod
        except ReadTimeoutError:
            # if watch timeout error is reached we might have started the event
            # generator too late and actually missed the event. Thus try to get
            # the pod without actually waiting for a event to happen.
            try:
                res = self._c.list_namespaced_pod(
                    self.namespace, label_selector=f"job-name={job_name}"
                )
                return res.items[0]
            except IndexError:
                raise ContainerNotFound(
                    f"Could not find a pod associated with {job_name}!"
                )

    def get_exit_info(self, container):
        self._wait_for_status(container, "Succeeded")
        pod = self._get_pod(container)
        pod_status = pod.status
        container_status = pod_status.container_statuses[0]
        return (
            container_status.state.terminated.exit_code,
            container_status.state.terminated.message,
        )

    def remove_container(self, container):
        self._c_batch.delete_namespaced_job(
            container.metadata.name, self.namespace, body=client.V1DeleteOptions()
        )

    def stop_container(self, container):
        """This concept is not supported by kubernetes."""
        pass

    def get_container(self, name):
        response = self._c.list_namespaced_pod(
            self.namespace, field_selector=f"metadata.name={name}"
        )
        try:
            pod = response.items[0]
        except IndexError:
            raise ContainerNotFound(f"Could not find a pod with name {name}!")
        return pod

    def get_retry_count(self, task_id):
        try:
            job = self._get_job(task_id)
            current = int(job.metadata.labels["luigi_retries"])
            status = self._get_pod(job).status.phase
            if status != "Failed":
                return current
            else:
                return current + 1
        except ContainerNotFound:
            return 0
