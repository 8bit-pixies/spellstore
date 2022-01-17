"""
Exports Historical Feature Group to file.
"""


import os.path
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import and_, column, func, or_, table
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import scoped_session, sessionmaker

from spellbook.base import RepoConfig


class FeatureStore(object):
    def __init__(self, repo_config: RepoConfig, engine: Optional[Engine] = None, full_join=False):
        self.repo_config = repo_config
        self.engine = repo_config.engine if engine is None else engine
        self.full_join = full_join

    def get_feature_group(self, feature_list: List[str]):
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
        snapshot_date: Optional[datetime] = None,
        output_file: Optional[str] = None,
        entity_list=None,
        limit: Optional[int] = None,
        chunksize=10000,
        force_fetch_all=False,
        force_append=False,
        verbose=False,
    ):
        if snapshot_date is None:
            snapshot_date = datetime.now()

        feature_group = self.get_feature_group(feature_list)
        query = feature_group.build_query(self.engine, snapshot_date=snapshot_date, entity_list=entity_list)
        output = ""

        header = True if not force_append else False
        if not force_fetch_all:
            # do something like - should add tqdm
            conn = self.engine.connect().execution_options(stream_results=True)  # type: ignore
            for chunk_df in pd.read_sql_query(query.statement, conn, chunksize=chunksize):
                if header:
                    output = chunk_df.to_markdown(index=False)

                if output_file is not None and output_file != "":
                    chunk_df.to_csv(output_file, mode="a", header=header)
                else:
                    break
                header = False
        else:
            df = pd.read_sql_query(query.statement, self.engine)
            output = df.to_markdown(index=False)
            if output_file is not None and output_file != "":
                output_mode = "a" if force_append else "w"

                df.to_csv(output_file, mode=output_mode, header=header)
        return output

    def join(
        self,
        entity_df,
        entity_column="",
        event_timestamp_column="",
        feature_list: List[str] = [],
        snapshot_date: Optional[datetime] = None,
        output_file: Optional[str] = None,
        limit: Optional[int] = None,
        chunksize=10000,
        force_fetch_all=False,
        force_append=False,
        verbose=False,
    ):
        """
        If snapshot date is provided, will just filter based on snapshot date + entity list,
        otherwise will attempt to group by entity_df + event_timestamp, and chunk it down.
        """
        # feature_group = self.get_feature_group(feature_list, snapshot_date)
        # return feature_group
        if entity_df.shape[0] <= 1000:
            force_fetch_all = True
        output: List[pd.DataFrame] = []
        if snapshot_date is not None or event_timestamp_column is None:
            # refactor this later
            entity_list = list(entity_df[entity_column])

            num_splits = (len(entity_list) // 999) + 1
            entity_list_splits = np.array_split(entity_list, num_splits)

            for elist in entity_list_splits:
                sub_entity_df = entity_df[entity_df[entity_column].isin(elist)]
                feature_group = self.get_feature_group(feature_list)
                query = feature_group.build_query(self.engine, snapshot_date=snapshot_date, entity_list=elist)

                temp_df = pd.read_sql_query(query.statement, self.engine)
                right_key = feature_group.feature_views[0].entity_column
                right_suffix = "_y"
                while any([x.endswith(right_suffix) for x in list(temp_df.columns) + list(sub_entity_df)]):
                    right_suffix = "_" + right_suffix
                temp_df = sub_entity_df.merge(
                    temp_df, how="left", left_on=entity_column, right_on=right_key, suffixes=(None, right_suffix)
                )
                keep_cols = [x for x in temp_df.columns if not x.endswith(right_suffix)]
                temp_df = temp_df[keep_cols]

                if force_fetch_all or len(output) == 0:
                    output.append(temp_df.copy())
                elif output_file is None:
                    raise ValueError("TODO fill this in, either you force fetch, or provide somewhere to spool")
                else:
                    header = not os.path.exists(output_file)
                    temp_df.to_csv(output_file, mode="a", header=header)

            if len(output) > 0:
                output = pd.concat(output)
            return output

        # otherwise entity_df is a dataframe, and we have to group by and chunk by event_timestamp
        for _, group_df in entity_df.groupby([entity_column, event_timestamp_column]):
            # refactor this later
            entity_list = list(group_df[entity_column])
            num_splits = (len(entity_list) // 1000) + 1
            entity_list_splits = np.array_split(entity_list, num_splits)
            temp_snapshot_date = group_df[event_timestamp_column].tolist()[0]

            for elist in entity_list_splits:
                sub_entity_df = group_df[group_df[entity_column].isin(elist)]
                feature_group = self.get_feature_group(feature_list)
                query = feature_group.build_query(self.engine, snapshot_date=temp_snapshot_date, entity_list=elist)
                # query = feature_group.build_query(self.engine, snapshot_date=temp_snapshot_date, entity_list=None)

                temp_df = pd.read_sql_query(query.statement, self.engine)

                right_key = feature_group.feature_views[0].entity_column
                right_suffix = "_y"
                while any([x.endswith(right_suffix) for x in list(temp_df.columns) + list(sub_entity_df)]):
                    right_suffix = "_" + right_suffix
                temp_df = sub_entity_df.merge(
                    temp_df, how="left", left_on=entity_column, right_on=right_key, suffixes=(None, right_suffix)
                )
                keep_cols = [x for x in temp_df.columns if not x.endswith(right_suffix)]
                temp_df = temp_df[keep_cols]

                if force_fetch_all:
                    output.append(temp_df.copy())
                elif output_file is None:
                    raise ValueError("TODO fill this in, either you force fetch, or provide somewhere to spool")
                else:
                    header = not os.path.exists(output_file)
                    temp_df.to_csv(output_file, mode="a", header=header)

        if len(output) > 0:
            output = pd.concat(output)
        return output


class FeatureView(BaseModel):
    name: str
    columns: List[str]
    entity_column: str = ""
    event_timestamp_column: Optional[str] = None
    create_timestamp_column: Optional[str] = None
    rank_column: Optional[str] = None

    def build_subquery_safe(self, engine, snapshot_date=None, entity_list=None):
        """
        A "safe" version by SQL verb support which avoids over + partition by
        """
        db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
        columns = self.columns.copy()
        self.rank_column = None
        if self.entity_column not in columns:
            columns.append(self.entity_column)

        if self.event_timestamp_column is None:
            self.columns = columns
            self.rank_column = None
            query_builder = db.query(table(self.name, *[column(col) for col in self.columns]))
        else:
            if self.event_timestamp_column not in columns:
                columns.append(self.event_timestamp_column)
            if self.create_timestamp_column not in columns and self.create_timestamp_column is not None:
                columns.append(self.create_timestamp_column)

            rank_col = "rnk"
            while rank_col in columns:
                rank_col = "r" + rank_col
            self.columns = columns

            if self.create_timestamp_column is None:
                subq = (
                    db.query(
                        table(self.name, column(self.entity_column)),
                        func.max(column(self.event_timestamp_column)).label(rank_col),
                    )
                    .filter(column(self.event_timestamp_column) <= snapshot_date)
                    .group_by(column(self.entity_column))
                )
                if entity_list is not None:
                    if type(entity_list) is not list:
                        entity_list = entity_list.tolist()  # avoid nd-arrays
                    subq = subq.filter(column(self.entity_column).in_(entity_list))
                subq = subq.subquery()

                query_builder = db.query(table(self.name, *[column(col) for col in self.columns])).join(
                    subq,
                    and_(
                        getattr(table(self.name, column(self.entity_column)).c, self.entity_column)
                        == getattr(subq.c, self.entity_column),
                        getattr(table(self.name, column(self.event_timestamp_column)).c, self.event_timestamp_column)
                        == getattr(subq.c, rank_col),
                    ),
                )
            else:

                subq = (
                    db.query(
                        table(self.name, column(self.entity_column)),
                        func.max(column(self.event_timestamp_column)).label(rank_col),
                        func.max(column(self.event_timestamp_column)).label(rank_col + "0"),
                    )
                    .filter(column(self.event_timestamp_column) <= snapshot_date)
                    .group_by(column(self.entity_column))
                )
                if entity_list is not None:
                    if type(entity_list) is not list:
                        entity_list = entity_list.tolist()  # avoid nd-arrays
                    subq = subq.filter(column(self.entity_column).in_(entity_list))
                subq = subq.subquery()

                query_builder = db.query(table(self.name, *[column(col) for col in self.columns])).join(
                    subq,
                    and_(
                        getattr(table(self.name, column(self.entity_column)).c, self.entity_column)
                        == getattr(subq.c, self.entity_column),
                        getattr(table(self.name, column(self.event_timestamp_column)).c, self.event_timestamp_column)
                        == getattr(subq.c, rank_col),
                        getattr(table(self.name, column(self.create_timestamp_column)).c, self.create_timestamp_column)
                        == getattr(subq.c, rank_col + "0"),
                    ),
                )

        return query_builder.subquery()

    def build_subquery(self, engine, snapshot_date=None, entity_list=None):
        db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
        columns = self.columns.copy()
        if self.entity_column not in columns:
            columns.append(self.entity_column)

        # if self.event_timestamp_column is None and entity_list is None:
        #     self.columns = columns
        #     self.rank_column = None
        #     query_builder = db.query(table(self.name, *[column(col) for col in self.columns]))
        if self.event_timestamp_column is None:
            self.columns = columns
            self.rank_column = None
            query_builder = db.query(table(self.name, *[column(col) for col in self.columns]))
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
                query_builder = db.query(
                    table(self.name, *[column(col) for col in self.columns]),
                    func.rank()
                    .over(order_by=column(self.event_timestamp_column).desc(), partition_by=self.entity_column)
                    .label(rank_col),
                ).filter(column(self.event_timestamp_column) <= snapshot_date)

            else:
                query_builder = db.query(
                    table(self.name, *[column(col) for col in self.columns]),
                    func.rank()
                    .over(
                        order_by=and_(
                            column(self.event_timestamp_column).desc(), column(self.create_timestamp_column).desc()
                        ),
                        partition_by=self.entity_column,
                    )
                    .label(rank_col),
                ).filter(column(self.event_timestamp_column) <= snapshot_date)

        if entity_list is not None:
            if type(entity_list) is not list:
                entity_list = entity_list.tolist()  # avoid nd-arrays
            query_builder = query_builder.filter(column(self.entity_column).in_(entity_list))

        return query_builder.subquery()


class FeatureGroup(BaseModel):
    feature_views: List[FeatureView]
    full_join: bool = True
    use_safe: bool = False

    def build_query(self, engine, snapshot_date=None, entity_list=None):
        db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
        table_dict = {}
        select_cols = []
        select_col_entity = []
        table_join_info = {}
        is_base_table = True
        base_entity_column = ""

        # build subqueries
        for fv in self.feature_views:
            if self.use_safe:
                table_dict[fv.name] = fv.build_subquery_safe(db, snapshot_date, entity_list)
            else:
                table_dict[fv.name] = fv.build_subquery(db, snapshot_date, entity_list)
            table_join_info[fv.name] = fv.entity_column
            print(dir(table_dict[fv.name].c), table_dict[fv.name].c.keys())
            select_cols.extend([getattr(table_dict[fv.name].c, col) for col in fv.columns if col != fv.entity_column])
            select_col_entity.append(getattr(table_dict[fv.name].c, fv.entity_column))
            if is_base_table:
                base_entity_column = fv.entity_column
            is_base_table = False

        # build id column via coalesce
        if len(select_col_entity) > 1:
            select_cols = [func.coalesce(*select_col_entity).label(base_entity_column)] + select_cols
        else:
            select_cols = [
                getattr(table_dict[self.feature_views[0].name].c, self.feature_views[0].entity_column)
            ] + select_cols
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

            # ensures properly joined for subsequent queries
            select_col_entity.append(getattr(table_dict[fv.name].c, fv.entity_column))

        return base_query
