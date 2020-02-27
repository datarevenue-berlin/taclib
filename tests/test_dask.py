from taclib.dask import compute, gather

try:
    import pytest
    import numpy as np
    import pandas as pd
    import dask.dataframe as dd
    import pandas.util.testing as pdt
    SKIP_DASK=False
except ImportError:
    SKIP_DASK = True


@pytest.mark.skipif(SKIP_DASK, reason='dask[dataframe] dependencies not installed')
def test_compute(client):
    df = pd.DataFrame({"A": np.arange(1000), "B": list("BA" * 500)})
    ddf = dd.from_pandas(df, npartitions=10)
    ddf_faulty = ddf.map_partitions(lambda x: x + 1, meta=ddf._meta)

    res = compute(ddf)

    pdt.assert_frame_equal(res, df)

    with pytest.raises(TypeError):
        compute(ddf_faulty, debug=True, recreate_error_locally=True)


@pytest.mark.skipif(SKIP_DASK, reason='dask[dataframe] dependencies not installed')
def test_gather(tmpdir, client):
    df = pd.DataFrame({"A": np.arange(1000), "B": list("BA" * 500)})
    ddf = dd.from_pandas(df, npartitions=10)
    ddf_faulty = ddf.map_partitions(lambda x: x + 1, meta=ddf._meta)
    # this returns a delayed object..
    futures = ddf_faulty.to_parquet(str(tmpdir), compute=False)

    with pytest.raises(TypeError):
        res = gather(futures)

    # ...so it also should work with compute
    with pytest.raises(TypeError):
        res = compute(futures, debug=True)
