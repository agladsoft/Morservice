import contextlib
import csv
import math
import sys
import time
import os
import httpx
import app_logger
from datetime import datetime
import pandas as pd
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
import yaml

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))

PARAMETRS = ['LIDER LINE']


class Morservice:

    def __init__(self):
        self.client = self.connect_db()
        self.month, self.year, self.is_ref, self.start = self.get_month_year()
        self.delta_teu = self.get_delta_teu()
        self.data_no_count, self.data_no = self.get_not_coincidences_in_db_positive()
        self.data_di_count, self.data_di = self.get_discrepancies_in_db_positive()

    def connect_db(self):
        try:
            logger.info('Подключение к базе данных')
            client: Client = get_client(host='clickhouse', database='default',
                                        username="default", password="6QVnYsC4iSzz")
        except httpx.ConnectError as ex_connect:
            logger.info(f'Wrong connection {ex_connect}')
            sys.exit(1)
        return client

    def get_month_year(self):
        result = self.client.query(
            f"Select * from check_month")
        data_loaded = result.result_rows
        month = data_loaded[0][1]
        year = data_loaded[0][2]
        is_ref = data_loaded[0][3]
        start = data_loaded[0][4]

        return month, year, is_ref, start

    def get_not_coincidences_in_db_positive(self, ref=False):
        logger.info('Получение delta_count из представления not_coincidences_by_params')
        result = self.client.query(
            f"Select * from not_coincidences_by_params where delta_count > 0 and month = '{self.month}' and year = '{self.year}'")
        data = result.result_rows

        # Получаем список имен столбцов
        column_names = result.column_names

        # Преобразуем результат в DataFrame
        df = pd.DataFrame(data, columns=column_names)
        if not ref:
            df = self.sort_params(df)
        return sum(df['delta_count'].to_list()), df

    def get_discrepancies_in_db_positive(self, ref=False):
        logger.info('Получение delta_count из представления not_coincidences_by_params')
        result = self.client.query(
            f"Select * from discrepancies_found_containers where delta_count > 0 and month = '{self.month}' and year = '{self.year}'")
        data = result.result_rows

        # Получаем список имен столбцов
        column_names = result.column_names

        # Преобразуем результат в DataFrame
        df = pd.DataFrame(data, columns=column_names)
        if not ref:
            df = self.sort_params(df)
        return sum(df['delta_count'].to_list()), df

    def sort_params(self, df):

        df['operator'] = df['operator'].str.upper().str.strip()
        filter_df = df.loc[~df['operator'].isin(PARAMETRS)]
        return filter_df

    def get_discrepancies_in_db_negative(self):
        '''Получение данных из discrepancies_found_containers которые меньше 0'''
        result = self.client.query("Select * from discrepancies_found_containers where delta_count < 0")

        data = result.result_rows

        # Получаем список имен столбцов
        column_names = result.column_names

        # Преобразуем результат в DataFrame
        df = pd.DataFrame(data, columns=column_names)

        return df

    def not_percentage(self):
        logger.info('Получение процентного соотношения 40 футовых и 20 футовых контейнеров')
        percent40 = ((self.delta_teu / self.data_no_count) - 1) * 100
        return percent40

    def not_dis_percentage(self):
        sum_delta_count = sum([self.data_no_count, self.data_di_count])
        percent40 = ((self.delta_teu / sum_delta_count) - 1) * 100
        return percent40

    def get_data(self, data_di):
        data = {
            'line': data_di['operator'],
            'ship': data_di['ship_name_unified'],
            'terminal': 'НЛЭ',
            'date': data_di['atb_moor_pier']
        }
        return data

    def add_container(self, data, count, flag):
        if flag:
            data['container_type'] = 'HC'
            data['container_size'] = 40
        else:
            data['container_type'] = 'DC'
            data['container_size'] = 20
        data['count_container'] = count

    def add_columns_no(self, data, percent, count):
        logger.info('Заполнение данных по контейнерам между 40фт и 20 фт в процентном соотношение')
        data_result = []
        if count <= 0:
            return None
        feet_40 = round((count * percent) / 100)
        feet_20 = count - feet_40
        # Распределение данных по контейнерам 40 футовым
        data = self.get_data(data)
        self.add_container(data, feet_40, True)
        data_result.append(data)
        # Распределение данных по контейнерам 20 футовым
        data = self.get_data(data)
        self.add_container(data, feet_20, False)
        data_result.append(data)
        return data_result

    def add_columns(self, data_di, percent40):
        logger.info('Заполнение данных по контейнерам между 40фт и 20 фт в процентном соотношение')
        delta_count = data_di['delta_count']
        data_result = []
        if delta_count <= 0:
            return None
        feet_40 = round((delta_count * percent40) / 100)
        feet_20 = delta_count - feet_40
        # Распределение данных по контейнерам 40 футовым
        data = self.get_data(data_di)
        self.add_container(data, feet_40, True)
        data_result.append(data)
        # Распределение данных по контейнерам 20 футовым
        data = self.get_data(data_di)
        self.add_container(data, feet_20, False)
        data_result.append(data)
        return data_result

    def get_delta_teu(self):
        logger.info('Получение значения в delta_teo из nle_cross')
        result = self.client.query(
            f"SELECT teu_delta FROM nle_cross where `month` = {self.month} and `year` = {self.year} and direction = 'import' and is_ref = {self.is_ref} and is_empty = 0")
        delta_teu = result.result_rows[0][0] if result.result_rows else 0
        return delta_teu

    def write_to_table(self, data_result):
        values = []
        for data in data_result:
            line = data.get('line')
            ship = data.get('ship')
            terminal = data.get('terminal')
            date = data.get('date')
            type_co = data.get('container_type')
            size = data.get('container_size')
            count = data.get('count_container')
            if count <= 0:
                continue
            values.append(
                [line, ship, terminal, date, type_co, size, count, None, None, None])
        # query = "INSERT INTO default.extrapolate (line, ship, terminal, date, container_type, container_size, count_container, goods_name, tracking_country,tracking_seaport)VALUES"
        # query += ', '.join(values)
        if values:
            self.client.insert('extrapolate', values,
                           column_names=['line', 'ship', 'terminal', 'date', 'container_type', 'container_size',
                                         'count_container','goods_name','tracking_country','tracking_seaport'])
        # self.client.query(query)

    def del_negative_container(self, data):
        '''Заполнение данных в таблицу с негативными значениями'''
        vessel = data['ship_name_unified']
        operator = data['operator']
        atb_moor_pier = str(data['atb_moor_pier'])
        value_positive = int(data['total_volume_in'])
        value_negative = abs(data['delta_count'])
        value = value_positive + value_negative
        self.client.query(
            f"ALTER TABLE default.reference_spardeck UPDATE  total_volume_in = {value} where vessel = '{vessel}' and operator = '{operator}' and atb_moor_pier = '{atb_moor_pier}'")

    @staticmethod
    def change_20(data_result, different):
        '''Изменение 20 футового на 40 футовый контейнер'''
        index = data_result.index(max(data_result, key=len))
        data_result[index][-1]['count_container'] += abs(different)
        data_result[index][0]['count_container'] -= abs(different)
        return data_result

    @staticmethod
    def change_40(data_result, different):
        '''Изменение 40 футового на 20 футовый '''
        index = data_result.index(max(data_result, key=len))
        data_result[index][0]['count_container'] += abs(different)
        data_result[index][-1]['count_container'] -= abs(different)
        return data_result

    def sum_delta_count(self, data):
        '''Подсчёт полученной суммы delta_teo'''
        summa = 0
        for d in data:
            for i in d:
                if i['container_size'] == 40:
                    summa += 2 * i['count_container']
                else:
                    summa += 1 * i['count_container']
        return summa

    def distribution_teu(self, flag):
        if flag == 'dis':
            for index, row in self.data_di.iterrows():
                percent = round((row['delta_count'] / self.data_di_count) * 100)
                self.data_di.loc[index, 'percent'] = percent
        elif flag == 'not':
            for index, row in self.data_no.iterrows():
                percent = round((row['delta_count'] / self.data_no_count) * 100)
                self.data_no.loc[index, 'percent'] = percent

    def filling_in_data_no_dis(self, flag):
        data_list = []
        if flag == 'not':
            count_container_40 = (self.delta_teu // 2) // 2
            count_container_20 = count_container_40 * 2
            for index, row in self.data_no.iterrows():
                lst = []
                data = self.get_data(row)
                feet_40 = round((count_container_40 * row['percent']) / 100)
                self.add_container(data, feet_40, True)
                lst.append(data)
                data = self.get_data(row)
                feet_20 = round((count_container_20 * row['percent']) / 100)
                self.add_container(data, feet_20, False)
                lst.append(data)
                data_list.append(lst)
        elif flag == 'dis':
            count_container_40 = (self.delta_teu // 2) // 2
            count_container_20 = count_container_40 * 2
            for index, row in self.data_di.iterrows():
                lst = []
                data = self.get_data(row)
                feet_40 = round((count_container_40 * row['percent']) / 100)
                self.add_container(data, feet_40, True)
                lst.append(data)
                data = self.get_data(row)
                feet_20 = round((count_container_20 * row['percent']) / 100)
                self.add_container(data, feet_20, False)
                lst.append(data)
                data_list.append(lst)
        return data_list

    def filling_in_data(self, percent, df):
        logger.info('Заполнение данных в таблицу')
        data_list = []
        for index, d in df.iterrows():
            data_result = self.add_columns(d, percent)
            if data_result is None:
                continue
            data_list.append(data_result)
        return data_list

    def check_delta_teu(self, data_result):
        '''Сверка delta_teu с заполненным результатом и приравнивание к 0'''
        summa_result = self.sum_delta_count(data_result)
        different = int(self.delta_teu) - summa_result
        if summa_result != int(self.delta_teu):
            if self.delta_teu > summa_result:
                data_result = self.change_20(data_result, different)
                if self.sum_delta_count(data_result) == self.delta_teu:
                    return data_result
            elif self.delta_teu < summa_result:
                data_result = self.change_40(data_result, different)
                if self.sum_delta_count(data_result) == self.delta_teu:
                    return data_result
        else:
            return data_result

    def write_result(self, data_list):
        for data in data_list:
            self.write_to_table(data)

    def work_to_data(self):
        '''Основная функция для работы с данными'''
        logger.info('Start working')
        if self.start:
            if self.delta_teu > 0:
                percent40_not = self.not_percentage()
                # Если суммы по линии not достаточно для покрытия 55/45 delta-teu закрываем только данной выборкой
                if 45 <= percent40_not <= 55:
                    '''Заполняем данные по линиям согласно данного процентного соотношения только по not'''
                    data_result = self.filling_in_data(percent40_not, self.data_no)
                    self.check_delta_teu(data_result)
                    self.write_result(data_result)

                elif percent40_not <= 0:
                    self.distribution_teu('not')
                    # Распределить согласнно данного teu количевсто контейнеров
                    data_result = self.filling_in_data_no_dis('not')
                    self.check_delta_teu(data_result)
                    self.write_result(data_result)

                elif percent40_not > 0:
                    percent40_not_dis = self.not_dis_percentage()
                    if percent40_not_dis < 0:
                        data_result_no = self.filling_in_data(50, self.data_no)
                        self.delta_teu -= self.get_sum_delta_teu(data_result_no)
                        self.distribution_teu('dis')
                        data_result_dis = self.filling_in_data_no_dis('dis')
                        self.check_delta_teu(data_result_dis)
                        data_result = data_result_no + data_result_dis
                        self.write_result(data_result)
                    elif 100 >= percent40_not_dis > 0:
                        union_df = pd.concat([self.data_no, self.data_di])
                        data_result = self.filling_in_data(percent40_not_dis, union_df)
                        self.check_delta_teu(data_result)
                        self.write_result(data_result)
                    elif percent40_not_dis > 100:
                        union_df = pd.concat([self.data_no, self.data_di])
                        data_result = self.filling_in_data(50, union_df)
                        # self.check_delta_teu(data_result)
                        self.write_result(data_result)

    def get_sum_delta_teu(self, data_result):
        delta_teu = 0
        for data in data_result:
            delta_teu += data[0]['count_container'] * 2
            delta_teu += data[1]['count_container']
        return delta_teu


if __name__ == '__main__':
    Morservice().work_to_data()

# compare_values()
