"""
Exports Historical Feature Group to file.
"""


from datetime import datetime
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel
from sqlalchemy import and_, column, func, or_, table
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import scoped_session, sessionmaker

from spellbook.base import RepoConfig


class FeatureStore(object):
    def __init__(self, repo_config: RepoConfig, engine: Engine, full_join=False):
        self.repo_config = repo_config
        self.engine = engine
        self.full_join = full_join

    def get_feature_group(self, feature_list: List[str], snapshot_date: Optional[datetime] = None):
        if snapshot_date is None:
            snapshot_date = datetime.now()

        table_col_dict = {}  # type: ignore
        table_ordered = []
        feature_views = []
        for tbl_col in feature_list:
            tbl, col = tbl_col.rsplit(".", 1)
            table_col_dict[tbl] = table_col_dict.get(tbl, []) + [col]
            if tbl not in table_ordered:
                table_ordered.append(tbl)

        for tbl in table_ordered:
            event_col = self.repo_config.get_attr_from_group_name(tbl, "event_timestamp_column")
            entity_col = self.repo_config.get_attr_from_group_name(tbl, "entity")
            feature_views.append(
                FeatureView(
                    name=tbl, columns=table_col_dict[tbl], entity_column=entity_col, event_timestamp_column=event_col
                )
            )

        return FeatureGroup(feature_views=feature_views, full_join=self.full_join)

    def export(
        self,
        feature_list: List[str],
        output_file: Optional[str] = None,
        snapshot_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        chunksize=10000,
        force_fetch_all=False,
        verbose=False,
    ):
        if snapshot_date is None:
            snapshot_date = datetime.now()

        feature_group = self.get_feature_group(feature_list, snapshot_date)
        query = feature_group.build_query(self.engine)
        output = ""

        if not force_fetch_all:
            # do something like - should add tqdm
            conn = self.engine.connect().execution_options(stream_results=True)
            header = True
            for chunk_df in pd.read_sql_query(query.statement, conn, chunksize=chunksize):
                if header:
                    output = chunk_df.to_markdown(index=False)

                if output_file is not None:
                    chunk_df.to_csv(output_file, mode="a", header=header)
                else:
                    break
                header = False
        else:
            df = pd.read_sql_query(query.statement, self.engine)
            output = df.to_markdown(index=False)
            if output_file is not None:
                df.to_csv(output_file, model="w", header=True)
        return output


class FeatureView(BaseModel):
    name: str
    columns: List[str]
    entity_column: str = ""
    event_timestamp_column: Optional[str] = None
    create_timestamp_column: Optional[str] = None
    rank_column: Optional[str] = None

    def build_subquery(self, engine):
        db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
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
            if self.create_timestamp_column not in columns and self.create_timestamp_column is not None:
                columns.append(self.create_timestamp_column)

            rank_col = "rnk"
            while rank_col in columns:
                rank_col = "r" + rank_col
            self.columns = columns
            self.rank_column = rank_col

            if self.create_timestamp_column is None:
                return db.query(
                    table(self.name, *[column(col) for col in self.columns]),
                    func.rank()
                    .over(order_by=column(self.event_timestamp_column).desc(), partition_by=self.entity_column)
                    .label(rank_col),
                ).subquery()
            else:
                return db.query(
                    table(self.name, *[column(col) for col in self.columns]),
                    func.rank()
                    .over(
                        order_by=and_(
                            column(self.event_timestamp_column).desc(), column(self.create_timestamp_column).desc()
                        ),
                        partition_by=self.entity_column,
                    )
                    .label(rank_col),
                ).subquery()


class FeatureGroup(BaseModel):
    feature_views: List[FeatureView]
    full_join: bool = True

    def build_query(self, engine):
        db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
        table_dict = {}
        select_cols = []
        select_col_entity = []
        table_join_info = {}
        is_base_table = True
        base_entity_column = ""

        # build subqueries
        for fv in self.feature_views:
            table_dict[fv.name] = fv.build_subquery(db)
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
                    isouter=not self.full_join,
                )
            elif len(select_col_entity) == 1:
                base_query = base_query.join(
                    table_dict[fv.name],
                    getattr(table_dict[self.feature_views[0].name].c, base_entity_column)
                    == getattr(table_dict[fv.name].c, fv.entity_column),
                    full=self.full_join,
                    isouter=not self.full_join,
                )
            else:
                # we're looking at the first table!
                pass
            if fv.rank_column is not None:
                base_query = base_query.filter(
                    or_(
                        getattr(table_dict[fv.name].c, fv.rank_column) == 1,
                        getattr(table_dict[fv.name].c, fv.rank_column).is_(None),
                    )
                )
            select_col_entity.append(getattr(table_dict[fv.name].c, fv.entity_column))

        return base_query
