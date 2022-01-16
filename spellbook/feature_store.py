"""
Exports Historical Feature Group to file.
"""


import chunk
from datetime import datetime
from typing import List, Optional

import pandas as pd
from sqlalchemy import MetaData, column, func, select, table, text
from sqlalchemy.engine.base import Engine

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
