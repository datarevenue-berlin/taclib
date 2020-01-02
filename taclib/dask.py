import os
import logging

try:
    import distributed
    from distributed.client import futures_of
except ImportError:
    logging.warning("distributed module failed to import")


def compute(collection, debug=False, **kwargs):
    if debug or os.getenv("DEBUG_PM", False):
        return _compute(collection, recreate_error_locally=True, **kwargs)
    else:
        return collection.compute(**kwargs)


def _compute(collection, recreate_error_locally=True, **kwargs):
    """This function works exactly as dask.compute, but automatically
    recreates the error locally so that it can be inspected with pdb.
    """
    try:
        client = distributed.Client.current()
    except ValueError as e:
        if "No clients found" not in str(e):
            raise e
        client = None

    if client:
        collection = collection.persist()
        futures = futures_of(collection)
        distributed.wait(futures)
        gathered = gather(futures, recreate_error_locally)

        f, a = collection.__dask_postcompute__()
        res = f(gathered, *a)

        return res
    else:
        return collection.compute(recreate_error_locally=recreate_error_locally)


def gather(futures, recreate_error_locally=True):
    # Fail if distributed scheduler is not available
    client = distributed.Client.current()

    if any(f.status == "error" for f in futures) and recreate_error_locally:
        for f in futures:
            if f.status == "error":
                future = f
                break
        client.recreate_error_locally(future)
        raise RuntimeError(
            "Client did not raise any exception on failed "
            "future! This is probably due to a killed "
            "worker. If you have pdb enabled leave the "
            "shell open and inspect the failed graph in "
            "Bokeh!"
        )
    else:
        gathered = client.gather(futures)
    return gathered
