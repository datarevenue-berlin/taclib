import pytest
from taclib.container import K8sClient


@pytest.mark.parametrize(
    "desired, actual, warn",
    [
        ("Running", "Succeeded", False),
        ("Running", "Failed", True),
        ("Running", "Pending", True),
        ("Running", "Running", False),
        ("Running", "Unkown", True),
        ("Succeeded", "Succeeded", False),
        ("Succeeded", "Failed", True),
        ("Succeeded", "Pending", True),
        ("Succeeded", "Running", True),
        ("Succeeded", "Unknown", True),
    ],
)
def test_warn_pod_status_timeout(desired, actual, warn, caplog):
    K8sClient._warn_pod_status_timeout(desired, actual)
    if warn:
        assert "Timeout while waiting for status" in caplog.text
    else:
        assert "Timeout while waiting for status" not in caplog.text
