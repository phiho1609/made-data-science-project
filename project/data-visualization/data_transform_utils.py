import numpy as np
import pandas as pd
from datetime import date
import time

a = 73

def calc_weekday_variances(weekday_values: list):
    # print('calc_weekday_variances():', weekday_values)
    # Calc variances for 7 weekdays
    weekday_variances = [0] * 7
    for i in range(7):
        # deviation = np.var(weekday_values[i], ddof=1)
        deviation = np.std(weekday_values[i], ddof=1) if len(weekday_values[i]) >= 2 else 0
        weekday_variances[i] = deviation
        if np.isnan(deviation):
            print('Variance is Nan for values:', weekday_values[i])

    # print('variances:', weekday_variances)
    return weekday_variances


'''Calculates weekday deviations for each month from a single year of traffic data'''
def get_weekday_traffic_deviations(table_df: pd.DataFrame, only_workdays=False):
    
    monthly_weekday_variances = []
    weekday_values = []
    for i in range(7):
        weekday_values.append([])

    last_p_datetime = table_df.loc[0, 'timestamp']
    last_p_month = last_p_datetime.month

    # for i in range(len(table_df)):
    for row in table_df.itertuples(index=False):
        p_datetime = getattr(row, 'timestamp')
        p_month = p_datetime.month
        p_weekday = getattr(row, 'weekday')
        
        if p_month != last_p_month:
            weekday_variances = calc_weekday_variances(weekday_values)
            monthly_weekday_variances.append(weekday_variances)
            weekday_values.clear()
            for i in range(7):
                weekday_values.append([])
        
        p_traffic_cnt = 0
        p_traffic_cnt += getattr(row, 'car_dir1_cnt')
        p_traffic_cnt += getattr(row, 'bus_dir1_cnt')
        p_traffic_cnt += getattr(row, 'car_dir2_cnt')
        p_traffic_cnt += getattr(row, 'bus_dir2_cnt')
        
        is_workday = getattr(row, 'day_type') != 's' and p_weekday <= 5
        do_include_value = is_workday if only_workdays else True
        if do_include_value:
            weekday_values[p_weekday-1].append(p_traffic_cnt)
        
        last_p_month = p_month


    # Push last month
    weekday_vars_last_month = calc_weekday_variances(weekday_values)
    monthly_weekday_variances.append(weekday_vars_last_month)
    
    return monthly_weekday_variances


def get_avg_weekday_traffic_deviations(table_df: pd.DataFrame, only_workdays=False):
    start_time = time.time()
    monthly_avg_weekday_variance = []
    monthly_weekday_variances = get_weekday_traffic_deviations(table_df, only_workdays)
    # print('Weekday variances per month:', monthly_weekday_variances)
    for weekday_variances in monthly_weekday_variances:
        avg_variance = sum(weekday_variances) / len(weekday_variances) if len(weekday_variances) > 0 else 0
        # avg_variance = sum(weekday_variances) / len(weekday_variances)
        monthly_avg_weekday_variance.append(avg_variance)

    # print('Resulting avg variance per month:', monthly_avg_weekday_variance)
    # print('get_avg_weekday_traffic_deviations took', str(time.time()-start_time) + 's')
    return monthly_avg_weekday_variance


def get_monthly_avg_day_traffic(table_df: pd.DataFrame, only_workdays=False):
    start_time = time.time()
    monthly_daily_traffic_values = [[] for i in range(12)]
    
    # Gather all daily traffic counts for each month and put them in a list    
    for row in table_df.itertuples(index=False):
        # If only workdays shall be included, skip holidays and weekends
        if only_workdays:
            weekday = getattr(row, 'weekday')
            day_type = getattr(row, 'day_type')
            is_workday = day_type != 's' and weekday <= 5
            if not is_workday:
                continue
        # Gather all relevant traffic data
        traffic_cnt = 0
        traffic_cnt += getattr(row, 'car_dir1_cnt')
        traffic_cnt += getattr(row, 'car_dir2_cnt')
        traffic_cnt += getattr(row, 'bus_dir1_cnt')
        traffic_cnt += getattr(row, 'bus_dir2_cnt')
         
        month = getattr(row, 'timestamp').month
        monthly_daily_traffic_values[month-1].append(traffic_cnt)

    # Average the monthly traffic as per-day traffic
    monthly_avg_day_traffic = []
    for daily_traffic_counts in monthly_daily_traffic_values:
        daily_avg = sum(daily_traffic_counts) / len(daily_traffic_counts)
        monthly_avg_day_traffic.append(daily_avg)
        # print('Daily traffic for a month:', daily_traffic_counts)
        # print('soos')
        # print('Average:', daily_avg)
    # print('get_monthly_avg_day_traffic took', str(time.time()-start_time) + 's')
    return monthly_avg_day_traffic


def get_punctuality_of_year(trainline_table_df: pd.DataFrame, year: int):
    
    single_year_filter = (trainline_table_df['timeperiod_start'] >= str(date(year, 1, 1))) & (trainline_table_df['timeperiod_start'] <= str(date(year, 12, 1)))
    monthly_values_df = trainline_table_df.loc[single_year_filter]
    # print(monthly_values_df)
    monthly_punctuality = {}
    for row in monthly_values_df.itertuples(index=False):
        month_timestamp = getattr(row, 'timeperiod_start')
        punctuality = getattr(row, 'punctuality')
        # Add month (as number) and punctuality (as float) to dict
        try:
            monthly_punctuality.update({month_timestamp.month: float(punctuality.replace(',', '.'))})
        except ValueError:
            print('Punctuality value:', punctuality, 'not parsable to float! Skipped...')
        
    # print(monthly_punctuality)
    return dict(sorted(monthly_punctuality.items()))

def b():
    print('b() was called!')
    
    
# if __name__ == '__main__':
#     b()