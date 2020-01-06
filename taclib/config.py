import os
import confuse

config = confuse.Configuration("taclib", __name__)
config["image"] = os.environ.get("TACLIB_IMAGE", config["image"].get(str))
config["namespaces"]["default"] = os.environ.get(
    "TACLIB_DEFAULT_NAMESPACE", config["namespaces"]["default"].get(str)
)
