import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from spellbook.base import Entity, Feature, Group, RepoConfig
from spellbook.feature_store import FeatureStore


def test_entity_join():
    engine = create_engine("sqlite:///:memory:")
    df = pd.DataFrame({"a": [1, 1, 1, 1], "b": [1, 2, 3, 4], "c": ["a", "b", "c", "d"]})
    entity_df = pd.DataFrame({"a": [1, 1, 1, 1], "b": [0.9, 2.2, 2.8, 3], "d": [1, 2, 3, 4]})

    df.to_sql("test", con=engine)
    rc = RepoConfig(
        entities=[Entity(name="a", value_type=int)],
        groups=[
            Group(name="test", entity="a", features=[Feature(name="c", value_type=str)], event_timestamp_column="b")
        ],
    )

    fs = FeatureStore(repo_config=rc, engine=engine)

    output = fs.join(
        entity_df, entity_column="a", event_timestamp_column="b", feature_list=["test.c"], force_fetch_all=True
    )

    assert output["c"].tolist() == [np.nan, "b", "b", "c"]
