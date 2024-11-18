import calendar
import pandas as pd
from datetime import timedelta


def transform_hours_from_pay_periods_to_months(hours_df: pd.DataFrame):
    """
    Translates hours data from pay periods in the format to the equivalent values by months
    """

    # Map the rows in the per-pay-period data to per-month rows
    data = {}
    for _idx, df_row in hours_df.iterrows():
        start_date = df_row["start_date"]
        end_date = start_date + timedelta(days=13)

        # Calculate the fraction of the pay period in the start_date month
        # monthrange() returns weekday of first day of the month and number of days in month
        days_in_start_month = calendar.monthrange(start_date.year, start_date.month)[1]
        fraction = min(1.0, (days_in_start_month - start_date.day + 1) / 14)

        # Add values from data columns to the current row with index: (dept ID, month)
        _copy_fraction_of_hours_row(df_row, data, start_date, fraction)

        # If the end month is different, then do the same thing with the rest of the pay period
        if start_date.month != end_date.month:
            _copy_fraction_of_hours_row(df_row, data, end_date, 1 - fraction)

    ret = pd.DataFrame(data).T
    return ret.reset_index(drop=True)


def _copy_fraction_of_hours_row(df_row, data, date, fraction):
    """
    Copy column values from a data frame row, df_row, to a dict, data.
    Values are multiplied by factor, which represents the part of the original data
    that belongs to a particular year and month (specified by date).
    """
    month = f"{date.year:04d}-{date.month:02d}"
    row_id = f"{df_row['dept_wd_id']}; {month}"
    data[row_id] = data.get(row_id, {})
    data_row = data[row_id]
    data_row["month"] = month
    data_row["dept_wd_id"] = df_row["dept_wd_id"]
    data_row["dept_name"] = df_row["dept_name"]
    for col in [
        "reg_hrs",
        "overtime_hrs",
        "prod_hrs",
        "nonprod_hrs",
        "total_hrs",
    ]:
        # Multiply the pay period value by portion of the period in this month
        data_row[col] = data_row.get(col, 0) + df_row[col] * fraction

    # FTE has to be recalculated using a conversion factor of (14 days / days in month),
    # because the FTE depends on the total hours / number of total days
    days_in_month = calendar.monthrange(date.year, date.month)[1]
    data_row["total_fte"] = data_row.get("total_fte", 0) + df_row[
        "total_fte"
    ] * fraction * (14 / days_in_month)
