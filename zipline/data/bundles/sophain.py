import os
import sys

from logbook import Logger, StreamHandler
from numpy import empty
from pandas import DataFrame, read_csv, Index, Timedelta

from zipline.utils import paths
from zipline.utils.cli import maybe_show_progress
from . import core as bundles

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)


@bundles.register("sophain")
def do_sophain(
        environ,
        asset_db_writer,
        minute_bar_writer,
        daily_bar_writer,
        adjustment_db_writer,
        calendar,
        start_session,
        end_session,
        cache,
        show_progress,
        data_path
):
    csvdir = paths.daily_data(environ)
    symbols = sorted(item.split('.csv')[0]
                     for item in os.listdir(csvdir)
                     if '.csv' in item)
    if not symbols:
        raise ValueError("no <symbol>.csv* files found in %s" % csvdir)

    dtype = [('start_date', 'datetime64[ns]'),
             ('end_date', 'datetime64[ns]'),
             ('auto_close_date', 'datetime64[ns]'),
             ('symbol', 'object')]
    metadata = DataFrame(empty(len(symbols), dtype=dtype))

    writer = daily_bar_writer

    dividends = {"cash": DataFrame(columns=["sid", 'amount', 'ex_date', "record_date", "pay_date"]),
                 "stock": DataFrame(columns=["sid", "payment_sid", "ratio", "ex_date", "record_date", "pay_date"]),
                 'splits': DataFrame(columns=['sid', 'ratio', 'effective_date'])}

    sessions = calendar.sessions_in_range(start_session, end_session)
    writer.write(_pricing_iter(csvdir, symbols, metadata, sessions,
                               dividends, show_progress),
                 show_progress=show_progress)

    metadata['exchange'] = "SHSZ"
    asset_db_writer.write(equities=metadata)
    dividends['cash']['sid'] = dividends['cash']['sid'].astype(int)
    dividends['stock']['sid'] = dividends['stock']['sid'].astype(int)
    dividends['splits']['sid'] = dividends['splits']['sid'].astype(int)
    dividends['stock']['payment_sid'] = dividends['stock']['sid']

    adjustment_db_writer.write(splits=dividends['splits'], dividends=dividends["cash"],
                               stock_dividends=dividends["stock"])


def _pricing_iter(csvdir, symbols, metadata, sessions, dividends, show_progress):
    with maybe_show_progress(symbols, show_progress,
                             label='Loading custom pricing data: ') as it:
        files = os.listdir(csvdir)
        for sid, symbol in enumerate(it):
            logger.debug('%s: sid %s' % (symbol, sid))
            try:
                fname = [fname for fname in files
                         if '%s.csv' % symbol in fname][0]
            except IndexError:
                raise ValueError("%s.csv file is not in %s" % (symbol, csvdir))

            dfr = read_csv(os.path.join(csvdir, fname),
                           parse_dates=[0],
                           infer_datetime_format=True,
                           index_col=0).sort_index()
            dfr = dfr.loc[~dfr.index.duplicated(keep='last')]
            dfr = dfr[:-1]
            # print(dfr.tail())

            start_date = dfr.index[0]
            end_date = dfr.index[-1]
            print("start_date", start_date)

            # The auto_close date is the day after the last trade.
            ac_date = end_date + Timedelta(days=1)
            metadata.iloc[sid] = start_date, end_date, ac_date, symbol  # , "NYSE"

            print("end_date", end_date)
            # print(sessions)
            # print(dfr[dfr.index.duplicated()])

            dfr.index = dfr.index.normalize()
            dfr = dfr.reindex(sessions.tz_localize(None))[start_date:end_date]

            # 现金分红处理
            if 'cash_div' in dfr.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                div_cash = dfr[dfr['cash_div'] != 0.0]['cash_div']
                div_record = dfr[dfr['record_date'] != None]['record_date']
                div_pay = dfr[dfr['pay_date'] != None]['pay_date']
                div = DataFrame(data=div_cash.index.tolist(), columns=['ex_date'])
                div['record_date'] = div_record.tolist()
                # div['declared_date'] = NaT
                div['pay_date'] = div_pay.tolist()
                div['amount'] = div_cash.tolist()
                div['sid'] = sid
                div['amount'].fillna(0, inplace=True)
                div.fillna(0, inplace=True)
                # for i in div["pay_date"]:
                #     print(i)
                divc = dividends['cash']
                ind = Index(range(divc.shape[0], divc.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)
                dividends['cash'] = divc.append(div)

            # 送股数据处理
            if "stk_div" in dfr.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                div_stock = dfr[dfr["stk_div"] != 0.0]["stk_div"]
                div_record = dfr[dfr['record_date'] != 0.0]['record_date']
                div_list = dfr[dfr['div_listdate'] != 0.0]['div_listdate']
                div = DataFrame(data=div_cash.index.tolist(), columns=['ex_date'])
                div['record_date'] = div_record.tolist()
                # div['declared_date'] = NaT
                div['pay_date'] = div_list.tolist()
                div["ratio"] = div_stock.tolist()
                div['sid'] = sid
                div['ratio'].fillna(0, inplace=True)
                div.fillna(0, inplace=True)
                divs = dividends['stock']
                ind = Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)
                dividends['stock'] = divs.append(div)

            yield sid, dfr
