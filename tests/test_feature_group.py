import pandas as pd
from sqlalchemy import create_engine

from spellbook.feature_store import FeatureGroup, FeatureView


def test_time_travel():
    engine = create_engine("sqlite:///:memory:")
    df = pd.DataFrame({"a": [1, 1, 2, 3], "b": [4, 5, 5, 6], "c": ["a", "a", "b", "c"]})
    df1 = pd.DataFrame({"a": [1, 1, 2, 3], "d": [7, 8, 9, 0], "e": ["q", "w", "e", "r"]})

    df.to_sql("test", con=engine)
    df1.to_sql("test1", con=engine)

    feature_group = FeatureGroup(
        feature_views=[
            FeatureView(name="test", columns=list(df.columns), entity_column="a", event_timestamp_column="b"),
            FeatureView(name="test1", columns=list(df1.columns), entity_column="a", event_timestamp_column="d"),
        ],
        full_join=False,
    )

    # q = feature_group.feature_views[0].build_subquery()
    # db.query(q.c.a, q.c.b, q.c.c).filter(q.c.rnk == 1)

    query = feature_group.build_query(engine)
    df = pd.read_sql_query(query.statement, con=engine)
    assert df.shape[1] == 5
    assert set(df["a"].tolist()) == set([1, 2, 3])
    assert df.shape[0] == 3


def test_left_join():
    engine = create_engine("sqlite:///:memory:")
    df = pd.DataFrame({"a": [1, 1, 2, 3], "b": [4, 5, 5, 6], "c": ["a", "a", "b", "c"]})
    df1 = pd.DataFrame({"a": [5, 5, 2, 3], "d": [7, 8, 9, 0], "e": ["q", "w", "e", "r"]})

    df.to_sql("test", con=engine)
    df1.to_sql("test1", con=engine)

    feature_group = FeatureGroup(
        feature_views=[
            FeatureView(name="test", columns=list(df.columns), entity_column="a", event_timestamp_column="b"),
            FeatureView(name="test1", columns=list(df1.columns), entity_column="a", event_timestamp_column="d"),
        ],
        full_join=False,
    )

    # q = feature_group.feature_views[0].build_subquery()
    # db.query(q.c.a, q.c.b, q.c.c).filter(q.c.rnk == 1)

    query = feature_group.build_query(engine)
    df = pd.read_sql_query(query.statement, con=engine)
    print(df)
    assert set(df["a"].tolist()) == set([1, 2, 3])
    assert df.shape[0] == 3


def test_time_travel_with_create():
    engine = create_engine("sqlite:///:memory:")
    df = pd.DataFrame({"a": [1, 1, 2, 3], "b": [5, 5, 5, 6], "b1": [1, 2, 1, 1], "c": ["a", "c", "b", "c"]})
    df1 = pd.DataFrame({"a": [1, 1, 2, 3], "d": [7, 8, 9, 0], "b1": [1, 2, 1, 1], "e": ["q", "w", "e", "r"]})

    df.to_sql("test", con=engine)
    df1.to_sql("test1", con=engine)

    feature_group = FeatureGroup(
        feature_views=[
            FeatureView(
                name="test",
                columns=list(df.columns),
                entity_column="a",
                event_timestamp_column="b",
                create_timestamp_column="b1",
            ),
            FeatureView(
                name="test1",
                columns=list(df1.columns),
                entity_column="a",
                event_timestamp_column="d",
                create_timestamp_column="b1",
            ),
        ],
        full_join=False,
    )

    # q = feature_group.feature_views[0].build_subquery()
    # db.query(q.c.a, q.c.b, q.c.c).filter(q.c.rnk == 1)

    query = feature_group.build_query(engine)
    df = pd.read_sql_query(query.statement, con=engine)
    assert df.shape[1] == 7
    assert set(df["a"].tolist()) == set([1, 2, 3])
    assert df.shape[0] == 3
