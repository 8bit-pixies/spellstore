import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, text

engine = create_engine("sqlite:///:memory:")
df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})

df.to_sql("test", con=engine)

# test metadata reflect
metadata = MetaData(engine)
test = Table("test", metadata)  # textual query

tbl_name = "test"
select_query = ", ".join([f"{tbl_name}.{col}" for col in df.columns])
query = select([text(f"{select_query} from {tbl_name}")])

pd.read_sql(str(query), con=engine)
