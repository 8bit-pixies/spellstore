import pandas as pd
from sqlalchemy import create_engine

from spellbook.base import Entity, Feature, Group, RepoConfig
from spellbook.feature_store import FeatureStore


def test_entity_join():
    engine = create_engine("sqlite:///:memory:", echo=True)
    df = pd.DataFrame({"a": [1, 1, 1, 1], "b": [1, 2, 3, 4], "c": ["a", "b", "c", "d"]})

    df.to_sql("test", con=engine)
    rc = RepoConfig(
        entities=[Entity(name="a", value_type=int)],
        groups=[
            Group(name="test", entity="a", features=[Feature(name="c", value_type=str)], event_timestamp_column="b")
        ],
    )

    fs = FeatureStore(repo_config=rc, engine=engine)

    output = fs.export(["test.c"], 10)
    assert len(output.split("\n")) >= 3
