import os
import confuse

config = confuse.Configuration("taclib", __name__)
config['image'] = os.environ.get("IMAGE", config["image"].get(str))
