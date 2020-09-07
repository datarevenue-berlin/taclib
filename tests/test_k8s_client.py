from taclib.container import K8sClient


def test_make_jobspec():
    client = K8sClient()

    job_spec = client._make_job_spec(
        "test-job",
        "drtools/job:0.1.0",
        ["echo", "hello", "world"],
        {},
        ["PYTHONUNBUFFERED=True"],
        {"labels": {"mypipeline": "test"}},
        {
            "volumes": [
                {
                    "name": "az-secrets-volume",
                    "secret": {"secret_name": "az-credentials-yaml"},
                }
            ]
        },
        {},
        {
            "volume_mounts": [
                {
                    "name": "az-secrets-volume",
                    "mount_path": "/root/.config/vpforecast/config.yaml",
                }
            ]
        },
    )
    container = job_spec.spec.template.spec.containers[0].to_dict()
    assert job_spec.spec.template.spec.volumes[0] == {
        "name": "az-secrets-volume",
        "secret": {"secret_name": "az-credentials-yaml"},
    }
    assert container["env"][0] == {
        "name": "PYTHONUNBUFFERED",
        "value": "True",
        "value_from": None,
    }
    assert container["command"] == ["echo", "hello", "world"]
    assert container["image"] == "drtools/job:0.1.0"
    assert container["volume_mounts"] == {
        "mount_path": "/root/.config/vpforecast/config.yaml",
        "name": "az-secrets-volume",
    }
