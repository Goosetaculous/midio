import json
from contextlib import closing
from datetime import datetime, date
import xlsxwriter
import pymysql
import logging


log = logging.getLogger('mysql')


class MysqlClient(object):
    connection_cache = {}

    def __init__(self):
        """
        Constructor to MySql Object to handle all MySql connections and queries
        :param args_obj: ReportingArguments Object which includes common arguments needed to run queries
        """
        self.settings = dict(
            host='sql.midigator.com',
            port=3306,
            user='python',
            password='D$d$1U9YCzz3',
            cursorclass=pymysql.cursors.DictCursor
        )

    def __enter__(self):
        connection = self.connection
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    @property
    def connection(self):
        if not self.connection_cache.get('mysql'):
            self.connection_cache = {'mysql': pymysql.connect(**self.settings)}
            self.connection_cache['mysql'].autocommit(True)
        return self.connection_cache['mysql']

    def get_all_mids(self,):
        query = """
                select m.id as mid_id, m.mid, min(m.created_at) as created_at,cred.company_id
                from system_operations.mid m
                left join security.credentials cred on cred.portal_account_id = m.portal_account_id
                group by m.mid;
                """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def get_chargeback_counts(self, mid_ids):
        query = """
                select m.id as mid_id, count(distinct(ci.id)) as cb_count, DATE_FORMAT(date(ci.created_at), "%%M %%Y") as date
                from system_operations.chargeback_incident ci
                join system_operations.dispute_case dc on ci.dispute_case_id = dc.id
                join system_operations.mid m on m.id = ci.mid_id
                where m.id in %s
                and dc.status_code in (1, 2, 4, -1)
                group by ci.mid, MONTH(ci.created_at), YEAR(ci.created_at)
                order by m.created_at desc;
                """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(query, (mid_ids, ))
            return cursor.fetchall()

    def get_alerts_counts(self, mid_ids):
        query = """
                select m.id as mid_id, count(distinct(pc.id)) as alert_count, DATE_FORMAT(date(pc.created_at), "%%M %%Y") as date
                from system_operations.prevention_case pc
                join system_operations.mid m on m.id = pc.mid_id
                where m.id in %s
                group by pc.mid_id, MONTH(pc.created_at), YEAR(pc.created_at)
                order by m.created_at desc;
                """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(query, (mid_ids, ))
            return cursor.fetchall()


class MergeData(object):
    @classmethod
    def run(cls, data1, data2, key):
        """
        Merges two list of dictionaries into a single array of dictionaries
        :param data1: first list of dictionaries
        :param data2: second list of dictionaries
        :param key: the key to use to match data points together for the merge
        :return: Array object of dictionaries with both data1 and data2 merged
        """
        merged = {}
        for item in data1 + data2:
            if item[key] in merged:
                cls.merge_dict(merged[item[key]], item)
            else:
                merged[item[key]] = item
        return list(merged.values())

    @classmethod
    def merge_dict(cls, dict_1, dict_2):
        """
        Merges two nested dictionaries together
        :param dict_1: First dictionary
        :param dict_2: Second dictionary
        """
        for k in dict_2:
            if k in dict_1 and isinstance(dict_1[k], dict) and isinstance(dict_2[k], dict):
                cls.merge_dict(dict_1[k], dict_2[k])
            else:
                dict_1[k] = dict_2[k]


class MidIO(object):
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    @classmethod
    def month_list(cls, start_date, end_date):
        dates = [start_date, end_date]
        start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in dates]
        total_months = lambda dt: dt.month + 12 * dt.year
        mlist = []
        for tot_m in range(total_months(start) - 1, total_months(end)):
            y, m = divmod(tot_m, 12)
            mlist.append(datetime(y, m + 1, 1).strftime("%B %Y"))
        return mlist

    @classmethod
    def collapse_data(cls, data, key):
        result = {}
        for row in data:
            if not result.get(row.get('mid_id')):
                result[row.get('mid_id')] = {'mid_id': row.get('mid_id'), row.get('date'): {key: row[key]}}
            else:
                result[row.get('mid_id')].update(**{row.get('date'): {key: row[key]}})
        return list(result.values())

    @classmethod
    def normalize(cls, value):
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        return value

    @classmethod
    def dict_to_xls(cls, data, save_path, keys=None):
        """
        Saves an excel sheet from the data provided
        :param data: List of dictionary objects
        :param save_path: Full path to save the excel file
        :param keys: Will be used to generate the header for the excel sheet
                     If not provided the keys from the first item in the data param
                     will be used
        """
        if keys is None:
            keys = data[0].keys()
        workbook = xlsxwriter.Workbook(save_path)
        worksheet = workbook.add_worksheet()
        col_index = dict((x, i) for i, x in enumerate(keys))

        row_index = 0

        # Add header
        for key in keys:
            worksheet.write(row_index, col_index[key], key)
        # Add rows
        for row in data:
            row_index += 1
            for k, v in row.items():
                worksheet.write(row_index, col_index[k], cls.normalize(v))
        # Save xlsx
        workbook.close()
        return save_path

    def export_schema(self, data):
        result = []
        month_list = self.month_list(self.start_date, self.end_date)
        for row in data:
            mid_dict = {'created_at': row['created_at'], 'mid': row['mid'], 'company_id': row['company_id']}
            for month in month_list:
                mid_dict.update(**{f"{month} cb": row.get(month, {}).get('cb_count', ''),
                                   f"{month} alert": row.get(month, {}).get('alert_count', '')})
            result.append(mid_dict)
        return result

    def get_mids(self):
        with MysqlClient() as mysql:
            return mysql.get_all_mids()

    def get_chargebacks(self, mids):
        with MysqlClient() as mysql:
            return mysql.get_chargeback_counts(mids)

    def get_alerts(self, mids):
        with MysqlClient() as mysql:
            return mysql.get_alerts_counts(mids)

    def run(self):
        mids = self.get_mids()
        mid_ids = [x.get('mid_id') for x in mids
                   if datetime.strptime(self.start_date, "%Y-%m-%d") <= x.get('created_at') <= datetime.strptime(self.end_date, "%Y-%m-%d")
                   ]
        chargebacks = self.get_chargebacks(mid_ids)
        merged_cbs = self.collapse_data(chargebacks, 'cb_count')
        alerts = self.get_alerts(mid_ids)
        merged_alerts = self.collapse_data(alerts, 'alert_count')
        merged_mids_cbs = MergeData.run(mids, merged_cbs, 'mid_id')
        all_data_merged = MergeData.run(merged_mids_cbs, merged_alerts, 'mid_id')
        excel_data = self.export_schema(all_data_merged)
        export_path = f"/Users/josepharanez/Desktop/{self.start_date}_{self.end_date}_mid_IO.xlsx"
        keys = ['created_at', 'mid','company_id','January 2017 cb', 'January 2017 alert', 'February 2017 cb', 'February 2017 alert', 'March 2017 cb', 'March 2017 alert', 'April 2017 cb', 'April 2017 alert', 'May 2017 cb', 'May 2017 alert', 'June 2017 cb', 'June 2017 alert', 'July 2017 cb', 'July 2017 alert', 'August 2017 cb', 'August 2017 alert',  'September 2017 cb', 'September 2017 alert',  'October 2017 cb', 'October 2017 alert',  'November 2017 cb', 'November 2017 alert',  'December 2017 cb', 'December 2017 alert', 'January 2018 cb', 'January 2018 alert', 'February 2018 cb', 'February 2018 alert', 'March 2018 cb', 'March 2018 alert', 'April 2018 cb', 'April 2018 alert', 'May 2018 cb', 'May 2018 alert', 'June 2018 cb', 'June 2018 alert','July 2018 cb', 'July 2018 alert', 'August 2018 cb', 'August 2018 alert','September 2018 cb', 'September 2018 alert','October 2018 cb', 'October 2018 alert']
        self.dict_to_xls(excel_data, export_path, keys)
        print("SUCCESS!")


if __name__ == '__main__':
    mid = MidIO("2017-01-01", "2018-10-31")
    mid.run()
