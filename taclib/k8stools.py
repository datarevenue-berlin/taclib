import os
import click
import kubernetes
from taclib.container import K8sClient
import os
from functools import wraps
import pdb as native_pdb
from logging import getLogger

try:
    import ipdb

    IPDB_AVAILABLE = True
except ImportError:
    ipdb = None
    IPDB_AVAILABLE = False


def post_mortem_interact(original_func):
    """Decorator to automatically jump into post mortem debugging if function
    fails. Adds a boolean `pdb` argument to the function. By default it is
    switched off.

    It can be turned on globally by settings DEBUG_PM environment variable.
    """
    DEBUG_PM = os.getenv("DEBUG_PM", False) is not False

    @wraps(original_func)
    def new_fun(*args, pdb=False, **kwargs):
        log = getLogger(__name__)
        try:
            original_func(*args, **kwargs)
        except:
            log.exception("Failed to execute {}.")
            if pdb or DEBUG_PM:
                log.info("Starting post mortem debugger!")
                if IPDB_AVAILABLE:
                    ipdb.post_mortem()
                else:
                    native_pdb.post_mortem()
            else:
                raise

    return new_fun


@click.group("debug")
def cli():
    pass


@cli.command()
@click.argument("pod-name")
@click.option("--new-name", default=None)
@click.option("-t", "--tag", default=None)
@click.option("--namespace", default="default")
@click.option("--ipython", default=True)
def rerun(pod_name, new_name, tag, namespace, ipython):
    c = K8sClient()

    cV1 = c._c

    pod = c.get_container(pod_name)

    if new_name:
        pod.metadata.name = new_name
    else:
        pod.metadata.name += "-debug"

    pod.metadata.resource_version = None

    pod.spec.containers[0].tty = True
    pod.spec.containers[0].stdin = True

    pod.spec.containers[0].image = replace_tag(pod.spec.containers[0].image, tag)

    env_vars = pod.spec.containers[0].env or []

    env_vars.append(kubernetes.client.V1EnvVar(name="DEBUG_PM", value="true"))

    pod.spec.containers[0].env = env_vars

    if ipython:
        cmd = pod.spec.containers[0].command or []
        # if command's part contains double quote, enclose it in single quotes
        cmd = [f"'{x}'" if '"' in x else x for x in cmd]
        cmd = ["/bin/sh", "-c", "pip install ipdb; {}".format(" ".join(cmd))]
        pod.spec.containers[0].command = cmd

    pod = cV1.create_namespaced_pod(namespace=namespace, body=pod)

    print(f"Execute:\n\nkubectl attach -ti {pod.metadata.name}\n")


@cli.command()
@click.argument("job-name")
@click.option("--delete-pods", default=True)
def cleanup(job_name):
    # TODO: need to pass controller pod label to tasks
    pass


def replace_tag(image, tag):
    repo, tag_old = image.split(":")
    if not tag:
        tag_new = tag_old
    return f"{repo}:{tag_new}"


if __name__ == "__main__":
    cli()
