from locale import getlocale
from typing import List, Optional

import pandas as pd
from sqlalchemy import MetaData, Table, column, create_engine, func, select, table, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import load_only, scoped_session, sessionmaker

engine = create_engine("sqlite:///:memory:")
db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
df = pd.DataFrame({"a": [1, 1, 2, 3], "b": [4, 5, 5, 6], "c": ["a", "a", "b", "c"]})
df1 = pd.DataFrame({"a": [1, 1, 2, 3], "d": [7, 8, 9, 0], "e": ["q", "w", "e", "r"]})

df.to_sql("test", con=engine)
df1.to_sql("test1", con=engine)

# test metadata reflect
metadata = MetaData(engine)
test = Table("test", metadata)  # textual query

tbl_name = "test"
tbl_query = table(tbl_name, *[column(col) for col in df.columns])


def get_query_by_id_timestamp(tbl_name, cols, id_col, timestamp_col):
    rank_col = "rnk"
    while rank_col in cols:
        rank_col = "r" + rank_col
    subquery = db.query(
        table(tbl_name, *[column(col) for col in cols]),
        func.rank().over(order_by=column(timestamp_col).desc(), partition_by=column(id_col)).label(rank_col),
    ).subquery()
    query = db.query(*[getattr(subquery.c, col) for col in cols]).filter(column(rank_col) == 1)
    return query


# now build the partition over by
tbl1 = get_query_by_id_timestamp("test", list(df.columns), "a", "b")
tbl2 = get_query_by_id_timestamp("test1", list(df1.columns), "a", "d")

from pydantic import BaseModel


class FeatureView(BaseModel):
    name: str
    columns: List[str]
    entity_column: str
    event_timestamp_column: Optional[str] = None
    rank_column: Optional[str] = None

    def build_subquery(self):
        columns = self.columns.copy()
        if self.entity_column not in columns:
            columns.append(self.entity_column)
        if self.event_timestamp_column is None:
            self.columns = columns
            self.rank_column = None
            return db.query(table(self.name, *[column(col) for col in self.columns])).subquery()
        else:
            if self.event_timestamp_column not in columns:
                columns.append(self.event_timestamp_column)
            rank_col = "rnk"
            while rank_col in columns:
                rank_col = "r" + rank_col
            self.columns = columns
            self.rank_column = rank_col
            return db.query(
                table(self.name, *[column(col) for col in self.columns]),
                func.rank()
                .over(order_by=column(self.event_timestamp_column).desc(), partition_by=self.entity_column)
                .label(rank_col),
            ).subquery()


class FeatureGroup(BaseModel):
    feature_views: List[FeatureView]
    full_join: bool = True

    def build_query(self):
        table_names = [x.name for x in self.feature_views]
        table_dict = {}
        select_cols = []
        select_col_entity = []
        table_join_info = {}
        is_base_table = True
        base_entity_column = ""

        for fv in self.feature_views:
            table_dict[fv.name] = fv.build_subquery()
            table_join_info[fv.name] = fv.entity_column
            select_cols.extend([getattr(table_dict[fv.name].c, col) for col in fv.columns if col != fv.entity_column])
            select_col_entity.append(getattr(table_dict[fv.name].c, fv.entity_column))
            if is_base_table:
                base_entity_column = fv.entity_column
            is_base_table = False

        # build id column via coalesce
        select_cols = [func.coalesce(*select_col_entity).label(base_entity_column)] + select_cols
        base_query = db.query(*select_cols)

        # add join and filter/rank conditions
        is_base_table = True
        select_col_entity = []
        for fv in self.feature_views:
            if len(select_col_entity) > 1:
                base_query = base_query.join(
                    table_dict[fv.name],
                    func.coalesce(*select_col_entity) == getattr(table_dict[fv.name].c, fv.entity_column),
                    full=self.full_join,
                )
            elif len(select_col_entity) == 1:
                base_query = base_query.join(
                    table_dict[fv.name],
                    getattr(table_dict[self.feature_views[0].name].c, base_entity_column)
                    == getattr(table_dict[fv.name].c, fv.entity_column),
                    full=self.full_join,
                )
            else:
                # we're looking at the first table!
                pass
            if fv.rank_column is not None:
                base_query = base_query.filter(getattr(table_dict[fv.name].c, fv.rank_column) == 1)
            select_col_entity.append(getattr(table_dict[fv.name].c, fv.entity_column))

        return base_query


feature_group = FeatureGroup(
    feature_views=[
        FeatureView(name="test", columns=list(df.columns), entity_column="a", event_timestamp_column="b"),
        FeatureView(name="test1", columns=list(df1.columns), entity_column="a", event_timestamp_column="d"),
    ],
    full_join=False,
)

# q = feature_group.feature_views[0].build_subquery()
# db.query(q.c.a, q.c.b, q.c.c).filter(q.c.rnk == 1)

query = feature_group.build_query()

pd.read_sql_query(query.statement, con=engine)
