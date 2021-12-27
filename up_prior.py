import datetime
from datetime import timedelta
import csv
import sqlite3

DATE_FORMAT = '%Y-%m-%d'

# Connect to the database
conn = sqlite3.connect('yahoo_finance.db')

# create cursor
cursor=conn.cursor()

def historical_query(d):
    sub_query = "SELECT 1 as id, symbol,"
    sub_query += "max(case when (Trade  BETWEEN '"+d+" 09:00:00' AND '"+d+" 09:29:59') then close else NULL end) as 'C900',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 09:30:00' AND '"+d+" 09:59:59') then close else NULL end) as 'C930',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 10:00:00' AND '"+d+" 10:29:59') then close else NULL end) as 'C1000',"
    sub_query += "max(case when (Trade  BETWEEN '"+d+" 10:30:00' AND '"+d+" 10:59:59') then close else NULL end) as 'C1030',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 11:00:00' AND '"+d+" 11:29:59') then close else NULL end) as 'C1100',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 11:30:00' AND '"+d+" 11:59:59') then close else NULL end) as 'C1130',"
    sub_query += "max(case when (Trade  BETWEEN '"+d+" 12:00:00' AND '"+d+" 12:29:59') then close else NULL end) as 'C1200',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 12:30:00' AND '"+d+" 12:59:59') then close else NULL end) as 'C1230',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 13:00:00' AND '"+d+" 13:29:59') then close else NULL end) as 'C1300',"
    sub_query += "max(case when (Trade  BETWEEN '"+d+" 13:30:00' AND '"+d+" 13:59:59') then close else NULL end) as 'C1330',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 14:00:00' AND '"+d+" 14:29:59') then close else NULL end) as 'C1400',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 14:30:00' AND '"+d+" 14:59:59') then close else NULL end) as 'C1430',"
    sub_query += "max(case when (Trade  BETWEEN '"+d+" 15:00:00' AND '"+d+" 15:29:59') then close else NULL end) as 'C1500',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 15:30:00' AND '"+d+" 15:59:59') then close else NULL end) as 'C1530',"
    sub_query += "max(case when (Trade BETWEEN '"+d+" 16:00:00' AND '"+d+" 16:59:59') then close else NULL end) as 'C1600'"
    sub_query += "FROM history  GROUP BY symbol ORDER BY symbol"

    return sub_query


def convert(date):
    try:
          date = datetime.datetime.strptime(date, DATE_FORMAT)
          return date.date()
    except TypeError:
          return date



def daterange(start, end):

    def get_date(n):
        return datetime.datetime.strftime(convert(start) + timedelta(days=n), DATE_FORMAT)

    days = (convert(end) - convert(start)).days + 1
    if days <= 0:
        raise ValueError('The start date must be before the end date.')
    for n in range(0, days):
        yield get_date(n)


def historical_gain_by_date(date, gain='base', gain_type='up', count=True):

    sub_query = historical_query(date)

    sql = "SELECT  h.* FROM ("+sub_query+") as h"
    if gain_type != 'no':
        if gain == 'base':
            if gain_type == 'down':
                sql += " WHERE (h.C1030 - h.C930) <= 0 "
            else:
                sql += " WHERE (h.C1030 - h.C930) > 0 "
        else:
            if gain_type == 'down':
                sql += " WHERE h."+gain+"  <= h.C1030  AND  (h.C1030 - h.C930) <= 0"
            else:
                sql += " WHERE h."+gain+"  > h.C1030  AND  (h.C1030 - h.C930) > 0"
    else:
        sql += " WHERE h.C1030 IS NULL OR h.C930 IS NULL "

    if count:
        cursor.execute("SELECT  1 as id, count(*) as count FROM ("+sql+") as h")
        return cursor.fetchall()[0][1]
    else:
        cursor.execute(sql)
        return cursor.fetchall()



def stocks_up_prior_days_from_date(from_date, days=1):
    sql = None
    orig_days = days

    while days:
        query_date = datetime.datetime.strftime(convert(from_date) - timedelta(days=days), DATE_FORMAT)

        sub_query = "SELECT h.symbol, (((h.C1600 - h.C930) / h.C930) * 100) as percent FROM ("+historical_query(query_date)+") as h"
        sub_query += " HAVING percent > 0 "
        if sql:
            sql = "SELECT symbol FROM (" + sub_query + ") as h WHERE symbol IN (" + sql + ")"
        else:
            sql = "SELECT symbol FROM (" + sub_query + ") as h"

        days -= 1

    data = []
    columns = ['base', 'C1100', 'C1130', 'C1200', 'C1230', 'C1300', 'C1330', 'C1400', 'C1430', 'C1500', 'C1530', 'C1600']

    column = {'date': from_date, 'base': 0, 'C1100': 0, 'C1130': 0, 'C1200': 0, 'C1230': 0, 'C1300': 0, 'C1330': 0, 'C1400': 0, 'C1430': 0, 'C1500': 0, 'C1530': 0, 'C1600': 0}

    for col in columns:
        res = historical_gain_by_date(date=from_date, gain=col)
        column[col] = res
    data.append( column )

    return ['Up '+str(orig_days)+' Day/s Prior and Up60', data[0]['base'],
                                        data[0]['C1100'], round((data[0]['C1100'] / data[0]['base']) * 100, 0) if data[0]['C1100'] else 0,
                                        data[0]['C1130'], round((data[0]['C1130'] / data[0]['base']) * 100, 0) if data[0]['C1130'] else 0,
                                        data[0]['C1200'], round((data[0]['C1200'] / data[0]['base']) * 100, 0) if data[0]['C1200'] else 0,
                                        data[0]['C1230'], round((data[0]['C1230'] / data[0]['base']) * 100, 0) if data[0]['C1230'] else 0,
                                        data[0]['C1300'], round((data[0]['C1300'] / data[0]['base']) * 100, 0) if data[0]['C1300'] else 0,
                                        data[0]['C1330'], round((data[0]['C1330'] / data[0]['base']) * 100, 0) if data[0]['C1330'] else 0,
                                        data[0]['C1400'], round((data[0]['C1400'] / data[0]['base']) * 100, 0) if data[0]['C1400'] else 0,
                                        data[0]['C1430'], round((data[0]['C1430'] / data[0]['base']) * 100, 0) if data[0]['C1430'] else 0,
                                        data[0]['C1500'], round((data[0]['C1500'] / data[0]['base']) * 100, 0) if data[0]['C1500'] else 0,
                                        data[0]['C1530'], round((data[0]['C1530'] / data[0]['base']) * 100, 0) if data[0]['C1530'] else 0,
                                        data[0]['C1600'], round((data[0]['C1600'] / data[0]['base']) * 100, 0) if data[0]['C1600'] else 0 ]

def check_weekend(date):
    year, month, day  = (int(x) for x in date.split('-'))
    weekend = datetime.date(year, month, day).strftime("%A")
    #print(weekend)
    if weekend == 'Sunday' or weekend == 'Saturday':
        return True
    return False

def get_weekdays(date_from, date_to):
    weekdays = []
    
    for date in list(daterange(date_from, date_to)):
        if check_weekend(date):
            continue
        weekdays.append(date)
    return weekdays

def stocks_up_prior_days_from_dates(date_from, date_to, days=1):
    #print(days)
    data = []
    columns = ['base', 'C1100', 'C1130', 'C1200', 'C1230', 'C1300', 'C1330', 'C1400', 'C1430', 'C1500', 'C1530', 'C1600']
    orig_days = days

    for date in list(daterange(date_from, date_to)):

        if check_weekend(date):
            continue

        weekdays = get_weekdays(datetime.datetime.strftime(convert(date) - timedelta(days=30), DATE_FORMAT), date)

        sql = None

        row = {'date': date, 'base': 0, 'C1100': 0, 'C1130': 0, 'C1200': 0, 'C1230': 0, 'C1300': 0, 'C1330': 0, 'C1400': 0, 'C1430': 0, 'C1500': 0, 'C1530': 0, 'C1600': 0}
        days = orig_days

        while  days:
            #query_date = datetime.datetime.strftime(convert(date) - timedelta(days=days), DATE_FORMAT)
                
            query_date = weekdays[-(days + 1)]


            sub_query = "SELECT h.symbol, (((h.C1600 - h.C930) / h.C930) * 100) as percent FROM ("+historical_query(query_date)+") as h"
            sub_query += " HAVING percent > 0 "
            if sql:
                sql = "SELECT symbol FROM (" + sub_query + ") as h WHERE symbol IN (" + sql + ")"
            else:
                sql = "SELECT symbol FROM (" + sub_query + ") as h"

            days -= 1


        for col in columns:
            res = historical_gain_by_date(date=date, gain=col)
            row[col] = res
        data.append( row )
        print(row)

    with open('up_'+str(orig_days)+'_prior.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'C930-C1030', 'C1100', 'C1100 Percent', 'C1130', 'C1130 Percent', 'C1200', 'C1200 Percent', 'C1230', 'C1230 Percent', 'C1300', 'C1300 Percent',
                            'C1330', 'C1330 Percent', 'C1400', 'C1400 Percent', 'C1430', 'C1430 Percent', 'C1500', 'C1500 Percent', 'C1530', 'C1530 Percent', 'C1600', 'C1600 Percent'])

        average = {'C1100': 0, 'C1100': 0, 'C1130': 0, 'C1200': 0, 'C1230': 0, 'C1300': 0, 'C1330': 0, 'C1400': 0, 'C1430': 0, 'C1500': 0, 'C1530': 0, 'C1600': 0}

        for row in data:
            c1100p = round((row['C1100'] / row['base']) * 100, 0) if row['C1100'] else 0
            c1130p = round((row['C1130'] / row['base']) * 100, 0) if row['C1130'] else 0
            c1200p = round((row['C1200'] / row['base']) * 100, 0) if row['C1200'] else 0
            c1230p = round((row['C1230'] / row['base']) * 100, 0) if row['C1230'] else 0
            c1300p = round((row['C1300'] / row['base']) * 100, 0) if row['C1300'] else 0
            c1330p = round((row['C1330'] / row['base']) * 100, 0) if row['C1330'] else 0
            c1400p = round((row['C1400'] / row['base']) * 100, 0) if row['C1400'] else 0
            c1430p = round((row['C1430'] / row['base']) * 100, 0) if row['C1430'] else 0
            c1500p = round((row['C1500'] / row['base']) * 100, 0) if row['C1500'] else 0
            c1530p = round((row['C1530'] / row['base']) * 100, 0) if row['C1530'] else 0
            c1600p = round((row['C1600'] / row['base']) * 100, 0) if row['C1600'] else 0

            average['C1100'] += c1100p
            average['C1130'] += c1130p
            average['C1200'] += c1200p
            average['C1230'] += c1230p
            average['C1300'] += c1300p
            average['C1330'] += c1330p
            average['C1400'] += c1400p
            average['C1430'] += c1430p
            average['C1500'] += c1500p
            average['C1530'] += c1530p
            average['C1600'] += c1600p

            writer.writerow([row['date'], row['base'],
                                        row['C1100'], c1100p,
                                        row['C1130'], c1130p,
                                        row['C1200'], c1200p,
                                        row['C1230'], c1230p,
                                        row['C1300'], c1300p,
                                        row['C1330'], c1330p,
                                        row['C1400'], c1400p,
                                        row['C1430'], c1430p,
                                        row['C1500'], c1500p,
                                        row['C1530'], c1530p,
                                        row['C1600'], c1600p ])

        writer.writerow(['', '',
                            '', round(average['C1100'] / len(data), 0), '', round(average['C1130'] / len(data), 0),
                            '', round(average['C1200'] / len(data), 0), '', round(average['C1230'] / len(data), 0),
                            '', round(average['C1300'] / len(data), 0), '', round(average['C1330'] / len(data), 0),
                            '', round(average['C1400'] / len(data), 0), '', round(average['C1430'] / len(data), 0),
                            '', round(average['C1500'] / len(data), 0), '', round(average['C1530'] / len(data), 0),
                            '', round(average['C1600'] / len(data), 0)])



def historical_gain(date_from, date_to):


    data = []
    columns = ['base', 'C1100', 'C1130', 'C1200', 'C1230', 'C1300', 'C1330', 'C1400', 'C1430', 'C1500', 'C1530', 'C1600']
    for date in list(daterange(date_from, date_to)):

        year, month, day  = (int(x) for x in date.split('-'))
        day = datetime.date(year, month, day).strftime("%A")
        if day == 'Sunday' or day == 'Saturday':
            continue

        row = {'date': date, 'base': 0, 'C1100': 0, 'C1130': 0, 'C1200': 0, 'C1230': 0, 'C1300': 0, 'C1330': 0, 'C1400': 0, 'C1430': 0, 'C1500': 0, 'C1530': 0, 'C1600': 0}

        for col in columns:
            res = historical_gain_by_date(date=date, gain=col)
            row[col] = res
        data.append( row )
        print(row)

    with open('up60.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'C930-C1030', 'C1100', 'C1100 Percent', 'C1130', 'C1130 Percent', 'C1200', 'C1200 Percent', 'C1230', 'C1230 Percent', 'C1300', 'C1300 Percent',
                            'C1330', 'C1330 Percent', 'C1400', 'C1400 Percent', 'C1430', 'C1430 Percent', 'C1500', 'C1500 Percent', 'C1530', 'C1530 Percent', 'C1600', 'C1600 Percent'])

        average = {'C1100': 0, 'C1100': 0, 'C1130': 0, 'C1200': 0, 'C1230': 0, 'C1300': 0, 'C1330': 0, 'C1400': 0, 'C1430': 0, 'C1500': 0, 'C1530': 0, 'C1600': 0}

        for row in data:
            c1100p = round((row['C1100'] / row['base']) * 100, 0) if row['C1100'] else 0
            c1130p = round((row['C1130'] / row['base']) * 100, 0) if row['C1130'] else 0
            c1200p = round((row['C1200'] / row['base']) * 100, 0) if row['C1200'] else 0
            c1230p = round((row['C1230'] / row['base']) * 100, 0) if row['C1230'] else 0
            c1300p = round((row['C1300'] / row['base']) * 100, 0) if row['C1300'] else 0
            c1330p = round((row['C1330'] / row['base']) * 100, 0) if row['C1330'] else 0
            c1400p = round((row['C1400'] / row['base']) * 100, 0) if row['C1400'] else 0
            c1430p = round((row['C1430'] / row['base']) * 100, 0) if row['C1430'] else 0
            c1500p = round((row['C1500'] / row['base']) * 100, 0) if row['C1500'] else 0
            c1530p = round((row['C1530'] / row['base']) * 100, 0) if row['C1530'] else 0
            c1600p = round((row['C1600'] / row['base']) * 100, 0) if row['C1600'] else 0

            average['C1100'] += c1100p
            average['C1130'] += c1130p
            average['C1200'] += c1200p
            average['C1230'] += c1230p
            average['C1300'] += c1300p
            average['C1330'] += c1330p
            average['C1400'] += c1400p
            average['C1430'] += c1430p
            average['C1500'] += c1500p
            average['C1530'] += c1530p
            average['C1600'] += c1600p

            writer.writerow([row['date'], row['base'],
                                        row['C1100'], c1100p,
                                        row['C1130'], c1130p,
                                        row['C1200'], c1200p,
                                        row['C1230'], c1230p,
                                        row['C1300'], c1300p,
                                        row['C1330'], c1330p,
                                        row['C1400'], c1400p,
                                        row['C1430'], c1430p,
                                        row['C1500'], c1500p,
                                        row['C1530'], c1530p,
                                        row['C1600'], c1600p ])
            #writer.writerow(stocks_up_prior_days_from_date(row['date'], 1))
            #writer.writerow(stocks_up_prior_days_from_date(row['date'], 2))

        writer.writerow(['', '',
                            '', round(average['C1100'] / len(data), 0), '', round(average['C1130'] / len(data), 0),
                            '', round(average['C1200'] / len(data), 0), '', round(average['C1230'] / len(data), 0),
                            '', round(average['C1300'] / len(data), 0), '', round(average['C1330'] / len(data), 0),
                            '', round(average['C1400'] / len(data), 0), '', round(average['C1430'] / len(data), 0),
                            '', round(average['C1500'] / len(data), 0), '', round(average['C1530'] / len(data), 0),
                            '', round(average['C1600'] / len(data), 0)])


fromd = '2021-11-27'
tod = '2021-12-27'

historical_gain(fromd, tod)
stocks_up_prior_days_from_dates(fromd, tod, 1)
stocks_up_prior_days_from_dates(fromd, tod, 2)
stocks_up_prior_days_from_dates(fromd, tod, 3)

conn.close()