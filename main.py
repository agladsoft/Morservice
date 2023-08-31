import contextlib
import csv
import math
import sys
import time
import os
import app_logger
from datetime import datetime
import pandas as pd
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client


logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))

PARAMETRS = ['LIDER LINE']


class Morservice():

    def __init__(self):
        self.client = self.connect_db()

    def connect_db(self):
        try:
            client: Client = get_client(host='10.23.4.203', database='default',
                                        username="default", password="6QVnYsC4iSzz")
        except Exception as ex:
            logger.info(f'Wrong connection {ex}')
            sys.exit(1)
        return client

    def get_discrepancies_in_db_positive(self,ref = False):
        logger.info('Получение delta_count из представления not_coincidences_by_params')
        result = self.client.query("Select * from not_coincidences_by_params where delta_count > 0")
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

    def filling_percentage(self, delta_count, delta_teu):
        logger.info('Получение процентного соотношения 40 футовых и 20 футовых контейнеров')
        percent40 = delta_teu / delta_count
        if percent40 > 2:
            return
        percent40 = (percent40 - 1) * 100
        return percent40

    def get_data(self, data_di):
        data = {
            'line': data_di['operator'],
            'ship': data_di['ship_name_unified'],
            'terminal': 'НЛЭ',
            'date': data_di['atb_moor_pier']
        }
        return data

    def add_container(self, data, count,flag):
        if flag:
            data['container_type'] = 'HC'
            data['container_size'] = 40
        else:
            data['container_type'] = 'DC'
            data['container_size'] = 20
        data['count_container'] = count


    def add_columns(self, data_di, percent40):
        logger.info('Заполнение данных по контейнерам между 40фт и 20 фт в процентном соотношение')
        delta_count = data_di['delta_count']
        data_result = []
        if delta_count <= 0:
            return None
        feet_40 = round((delta_count * percent40) / 100)
        feet_20 = delta_count - feet_40
        #Распределение данных по контейнерам 40 футовым
        data = self.get_data(data_di)
        self.add_container(data,feet_40, True)
        data_result.append(data)
        #Распределение данных по контейнерам 20 футовым
        data = self.get_data(data_di)
        self.add_container(data, feet_20,False)
        data_result.append(data)
        return data_result

    def get_delta_teu(self):
        logger.info('Получение значения в delta_teo из nle_cross')
        result = self.client.query(
            "SELECT teu_delta FROM nle_cross nc where `month` = 5 and `year` = 2023 and direction = 'import' and is_ref = False and is_empty = 0")
        delta_teu = result.result_rows[0][0] if result.result_rows else 0
        return delta_teu

    def write_to_table(self, data_result):
        values = []
        for data in data_result:
            line = data['line']
            ship = data['ship']
            terminal = data['terminal']
            date = data['date']
            type_co = data['container_type']
            size = data['container_size']
            count = data['count_container']
            values.append(f"('{line}', '{ship}', '{terminal}', '{date}', '{type_co}', {size}, {count}, Null, Null, Null)")

        query = "INSERT INTO default.test_table (line, ship, terminal, date, container_type, container_size, count_container, goods_name, tracking_country,tracking_seaport)VALUES"
        query += ', '.join(values)
        self.client.query(query)

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
    def change_20(data_result,different):
        '''Изменение 20 футового на 40 футовый контейнер'''
        index = data_result.index(max(data_result, key=len))
        data_result[index][-1]['count_container'] += abs(different)
        data_result[index][0]['count_container'] -= abs(different)
        return data_result

    @staticmethod
    def change_40(data_result,different):
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

    def check_delta_teu(self, data_result, delta_teo):
        '''Сверка delta_teu с заполненным результатом и приравнивание к 0'''
        summa_result = self.sum_delta_count(data_result)
        different = int(delta_teo) -  summa_result
        if summa_result != int(delta_teo):
            if delta_teo > summa_result:
                data_result = self.change_20(data_result,different)
                if self.sum_delta_count(data_result) == delta_teo:
                    return data_result
            elif delta_teo < summa_result:
                data_result = self.change_40(data_result,different)
                if self.sum_delta_count(data_result) == delta_teo:
                    return data_result
        else:
            return data_result



    def work_to_data(self):
        '''Основная функция для работы с данными'''
        logger.info('Start working')
        delta_teu = self.get_delta_teu()
        if delta_teu > 0:
            delta_count, data_di = self.get_discrepancies_in_db_positive()
            percent40 = self.filling_percentage(delta_count, delta_teu)
            if percent40 is None:
                logger.info('Не достаточно контейнеров для покрытия delta_teo')
                return
            logger.info('Заполнение данных в таблицу')
            data_list = []
            for index, d in data_di.iterrows():
                data_result = self.add_columns(d, percent40)
                if data_result is None:
                    continue
                data_list.append(data_result)
            self.check_delta_teu(data_list, delta_teu)
            for data in data_list:
                self.write_to_table(data)
            # delta_negative = self.get_discrepancies_in_db_negative()
            # for index,d in delta_negative.iterrows():
            #     self.del_negative_container(d)






if __name__ == '__main__':
    Morservice().work_to_data()
# compare_values()
