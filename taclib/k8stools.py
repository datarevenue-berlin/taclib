import os

import click
import kubernetes

os.environ["DRTOOLS_SETTINGS_MODULE"] = "smaato_dmo.settings.dev"

from drtools.core.container import K8sClient


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
