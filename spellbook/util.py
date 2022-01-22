from datetime import date, datetime, timedelta
from typing import Optional, Union


def infer_ttl_field(
    snapshot_date: Union[datetime, date, str, int, float], ttl: Optional[Union[int, float, timedelta]] = None
):
    # whatever type snapshot_date is, after we do snapshot_date - ttl, it must
    # be the same type, ttl shouldn't be None?
    if ttl is None:
        return None

    if isinstance(snapshot_date, (int, float)):
        if isinstance(ttl, (int, float)) and ttl > 0:
            return snapshot_date - ttl
        elif isinstance(ttl, (int, float)):
            return None
        else:
            raise ValueError(f"snapshot_date {type(snapshot_date)} and ttl {type(ttl)} are not compatible types!")
    elif isinstance(snapshot_date, (date, datetime)) and isinstance(ttl, timedelta):
        return snapshot_date - ttl
    elif type(snapshot_date) is str and isinstance(ttl, timedelta):
        # need to infer datetime or date format, to convert to and fro.
        date_formats = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S" "%Y-%m-%d"]
        for dt_fmt in date_formats:
            try:
                snp_date = datetime.strptime(snapshot_date, dt_fmt)
                ttl_date = snp_date - ttl
                return ttl_date.strftime(dt_fmt)
            except Exception:
                pass
        raise ValueError(f"Unable to process snapshot_date {snapshot_date} using ttl {ttl}")
    else:
        raise ValueError(f"snapshot_date {type(snapshot_date)} and ttl {type(ttl)} are not compatible types!")
