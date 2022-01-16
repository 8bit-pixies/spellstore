"""
Exports Historical Feature Group to file.
"""


import chunk
from datetime import datetime
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel
from sqlalchemy import MetaData, Table, column, create_engine, func, select, table, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import scoped_session, sessionmaker

from spellbook.base import RepoConfig


class FeatureStore(object):
    def __init__(self, repo_config: RepoConfig, engine: Engine):
        self.repo_config = repo_config
        self.engine = engine

    def export(
        self,
        feature_list: List[str],
        output_file: str,
        snapshot_date: Optional[datetime] = None,
        chunksize=10000,
        force_fetch_all=False,
        verbose=False,
    ):
        if snapshot_date is None:
            snapshot_date = datetime.now()
        # need to handle time-travel joins
        metadata = MetaData(self.engine)
        table_col_dict = {}
        table_ordered = []
        for tbl_col in feature_list:
            tbl, col = tbl_col.rsplit(".", 1)
            table_col_dict[tbl] = table_col_dict.get(tbl, []) + [col]
            if tbl not in table_ordered:
                table_ordered.append(tbl)

        table_ordered_query = []
        for tbl in table_ordered:
            event_col = self.repo_config.get_attr_from_group_name(tbl, "event_timestamp_column")
            entity_col = self.repo_config.get_attr_from_group_name(tbl, "entity")
            tbl_select = table(tbl, *[column(col) for col in table_col_dict[tbl]])
            if event_col is not None:
                tbl_select = tbl_select.filter(getattr(tbl_select, event_col) <= snapshot_date)
                # get the latest record only
                subq = tbl_select.query(
                    getattr(tbl_select, entity_col), func.max(getattr(tbl_select, event_col)).label("max_date")
                ).group_by(getattr(tbl_select, entity_col))

        if not force_fetch_all:
            # do something like - should add tqdm
            conn = self.engine.connect().execution_options(stream_results=True)
            header = True
            for chunk_df in pd.read_sql(query, conn, chunksize=chunksize):
                chunk_df.to_csv(output_file, mode="a", header=header)
                header = False
        else:
            pd.read_sql(query, conn).to_csv(output_file, model="w", header=True)
        return None


class FeatureView(BaseModel):
    name: str
    columns: List[str]
    entity_column: str
    event_timestamp_column: Optional[str] = None
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

    def build_query(self, engine):
        db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
        table_dict = {}
        select_cols = []
        select_col_entity = []
        table_join_info = {}
        is_base_table = True
        base_entity_column = ""

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
