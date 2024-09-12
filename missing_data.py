import math
import sys
import time
from typing import Tuple, Any

import pandas as pd
from app_logger import get_logger
from __init__ import *
from Database import ClickHouse
from Ref import Extrapolate

# logger: app_logger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))


class Missing(ClickHouse, Extrapolate):

    def __init__(self):
        self.client = self.connect_db()
        self.month, self.year, self.direction, self.is_missing_data, self.terminal = self.get_month_and_year_and_terminal()
        self.reference_region: DataFrame = self.get_reference_region()

    def get_month_and_year_and_terminal(self) -> Tuple[Any, Any, Any, Any, Any]:
        result = self.client.query('Select * from check_month')
        result = result.result_rows
        value = [value for value in result if value[5]]
        if len(value) != 1:
            sys.exit(1)
        _, month, year, direction, _, is_missing_data, terminal = value[0]
        return month, year, direction, is_missing_data, terminal

    @staticmethod
    def get_df(result: Sequence) -> DataFrame:
        data: Sequence = result.result_rows

        # Получаем список имен столбцов
        column_names: Sequence = result.column_names

        # Преобразуем результат в DataFrame
        df: DataFrame = pd.DataFrame(data, columns=column_names)

        return df

    def get_nle_cross(self):
        terminal = self.get_terminal()
        result = self.client.query(
            f"select * from nle_cross Where `month` = {self.month} and `year` = {self.year} and terminal = '{terminal}' and direction = '{self.direction}'")

        return self.get_df(result)

    def get_containers(self, table):
        'not_found_containers'
        'discrepancies_found_containers'
        result: QueryResult = self.client.query(
            f"Select * from {table} where delta_count >= 0 "
            f"and month = '{self.month}' and year = '{self.year}'"
            f" and direction = '{self.direction}' and stividor = '{self.terminal.upper()}'")
        return self.get_df(result)

    def check_missing_data(self):
        nle_cross = self.get_nle_cross()
        if sum(nle_cross['teu_delta']) <= 0:
            sys.exit(1)
        not_found_containers = self.get_containers('not_found_containers')
        discrepancies_found_containers = self.get_containers('discrepancies_found_containers')
        sum_not_found = sum(not_found_containers['delta_count']) if not not_found_containers.empty else 0
        sum_dis_found = sum(
            discrepancies_found_containers['delta_count']) if not discrepancies_found_containers.empty else 0
        if sum_not_found > 0 or sum_dis_found > 0:
            sys.exit(1)

        return not_found_containers, discrepancies_found_containers, nle_cross

    def get_dataframe_discrepancies(self):

        result: QueryResult = self.client.query(
            f"Select *  from discrepancies_found_containers where delta_count <= 0 "
            f"and month = '{self.month}' and year = '{self.year}'"
            f" and direction = '{self.direction}' and stividor = '{self.terminal.upper()}'")
        df = self.get_df(result)
        if self.direction == 'import':
            df.drop('total_volume_out', axis=1, inplace=True)
            df = df.rename({'total_volume_in': 'total'}, axis=1)
        else:
            df.drop('total_volume_in', axis=1, inplace=True)
            df = df.rename({'total_volume_out': 'total'}, axis=1)

        return df

    @staticmethod
    def count_containers_more(delta_teu):
        count_containers_20ft = delta_teu // 2
        count_containers_40ft = (delta_teu // 2) // 2
        sum_count_containers = sum([count_containers_20ft, (count_containers_40ft * 2)])
        if sum_count_containers != delta_teu:
            difference = delta_teu - sum_count_containers
            count_containers_20ft += difference
        return count_containers_20ft, count_containers_40ft

    def fill_extrapolate(self, df, delta_teu):
        count_containers_20ft, count_containers_40ft = self.count_containers_more(delta_teu)
        df['40ft'] = ((df['percent'] / 100) * count_containers_40ft).round().astype(int)
        df['20ft'] = ((df['percent'] / 100) * count_containers_20ft).round().astype(int)
        return df

    @staticmethod
    def fill_extrapolate_ref(df, delta_teu):
        count_containers_40ft = (delta_teu // 2)
        df['40ft'] = ((df['percent'] / 100) * count_containers_40ft).round().astype(int)
        return df

    def filling_data_ref(self, df, delta_teu):
        df.loc[:, 'percent'] = (df['total'] / df['total'].sum()) * 100
        df = self.fill_extrapolate_ref(df, delta_teu)
        df.loc[:, '20ft'] = 0
        df.loc[:, 'type'] = 'ref'
        return df

    def filling_data_full_empty(self, df, delta_teu, type):
        df.loc[:, 'percent'] = (df['total'] / df['total'].sum()) * 100
        df = self.fill_extrapolate(df, delta_teu)
        df.loc[:, 'type'] = type
        return df

    @staticmethod
    def check_equal(df, delta_teu):
        return delta_teu == (df['20ft'].sum() + (df['40ft'].sum() * 2))

    def control_count_container(self, df, delta_teu):
        # Функция для добавления контейнеров
        def add_containers(df, delta_teu_different):
            while delta_teu_different > 0:
                idx_max = df['total'].idxmax()

                # Если разница кратна 2, добавляем только 40ft контейнеры
                if delta_teu_different >= 2 and df.loc[idx_max, 'total'] - (
                        df.loc[idx_max, '20ft'] + df.loc[idx_max, '40ft'] * 2) >= 2:
                    df.loc[idx_max, '40ft'] += 1
                    delta_teu_different -= 2
                elif df.loc[idx_max, 'total'] - (df.loc[idx_max, '20ft'] + df.loc[idx_max, '40ft'] * 2) >= 1:
                    df.loc[idx_max, '20ft'] += 1
                    delta_teu_different -= 1
                else:
                    break
            return delta_teu_different

        # Функция для удаления контейнеров
        def remove_containers(df, delta_teu_different):
            while delta_teu_different > 0:
                idx_max = df['total'].idxmax()

                # Если разница кратна 2, убираем только 40ft контейнеры
                if delta_teu_different >= 2 and df.loc[idx_max, '40ft'] > 0:
                    df.loc[idx_max, '40ft'] -= 1
                    delta_teu_different -= 2
                elif df.loc[idx_max, '20ft'] > 0:
                    df.loc[idx_max, '20ft'] -= 1
                    delta_teu_different -= 1
                # Если не хватает 20ft, компенсируем с помощью 40ft
                elif df.loc[idx_max, '40ft'] > 0:
                    df.loc[idx_max, '40ft'] -= 1
                    delta_teu_different -= 2
                    df.loc[idx_max, '20ft'] += 1
                    delta_teu_different += 1  # Коррекция добавлением 20ft
                else:
                    # logger.error('Не обработанная логика')
                    sys.exit(1)
            return delta_teu_different

        delta_teu_different = (df['20ft'].sum() + (df['40ft'].sum() * 2)) - delta_teu
        # Если разница положительная — убираем контейнеры
        if delta_teu_different > 0:
            delta_teu_different = remove_containers(df, delta_teu_different)

        # Если разница отрицательная — добавляем контейнеры
        elif delta_teu_different < 0:
            delta_teu_different = add_containers(df, -delta_teu_different)

        # Проверяем, если все значения контейнеров равны требуемому delta_teu
        if self.check_equal(df, delta_teu):
            return df

        self.control_count_container(df, delta_teu_different, delta_teu)

    @staticmethod
    def get_container_type(container_type: str, container_size: int) -> str:
        if container_type == 'ref':
            return 'REF'
        if container_size == 20:
            return 'DC'
        else:
            return 'HC'

    @staticmethod
    def get_name_terminal(terminal: str):
        terminal = terminal.upper()
        if terminal == "NMTP":
            return "НМТП"
        elif terminal == "NLE":
            return "НЛЭ"

    def get_body(self, row: Series) -> List[Dict]:
        list_lines = []

        # Базовый словарь данных
        base_data = {
            'line': row['line_unified'],
            'ship': row['ship_name_unified'],
            'vessel': row['vessel'],
            'terminal': self.get_name_terminal(self.terminal),
            'date': row['shipment_date'],
            'is_empty': row['type'] == 'empty',
            'is_ref': row['type'] == 'ref',
            'is_missing': True,
        }

        # Для 20-футовых контейнеров
        data_20ft = base_data.copy()
        data_20ft.update({
            'container_type': self.get_container_type(row['type'], 20),
            'container_size': 20,
            'count_container': row['20ft']
        })
        list_lines.append(data_20ft)

        # Для 40-футовых контейнеров
        data_40ft = base_data.copy()
        data_40ft.update({
            'container_type': self.get_container_type(row['type'], 40),
            'container_size': 40,
            'count_container': row['40ft']
        })
        list_lines.append(data_40ft)

        return list_lines

    @staticmethod
    def add_month_year(line_tuple: List[dict], month: int, year: int) -> None:
        for line in line_tuple:
            line.update({'month_port': month, 'year_port': year})

    def add_port_in_line(self, lst_result: List[List[dict]]) -> List[List[dict]]:
        for line in lst_result:
            time.sleep(1)
            port: str = self.get_information_port(line)
            df_port: DataFrame
            month: int
            year: int
            df_port, month, year = self.get_popular_port(port)
            self.add_month_year(line, month, year)
            self.fill_line(line, df_port)
        return lst_result

    def finish_fill(self, result: List[Any]):
        finish_result = []
        result = pd.concat(result)
        for index, row in result.iterrows():
            finish_result.append(self.get_body(row))
        if self.terminal == 'nle':
            finish_result = self.add_port_in_line(finish_result)
        self.write_result(finish_result)

    def main(self):
        not_found_containers, discrepancies_found_containers, nle_cross = self.check_missing_data()
        # in - import
        # out - export
        ref_cross = int(nle_cross[nle_cross['is_ref'] == True]['teu_delta'].iloc[0])
        empty_cross = int(nle_cross[nle_cross['is_empty'] == True]['teu_delta'].iloc[0])
        full_cross = int(
            nle_cross[(nle_cross['is_ref'] == False) & (nle_cross['is_empty'] == False)]['teu_delta'].iloc[0])
        dis_df = self.get_dataframe_discrepancies()
        _, dis_ref = self.sort_ref_param(dis_df, True)
        _, dis_full = self.sort_ref_param(dis_df, False)
        result = []
        if ref_cross:
            fill_ref = dis_full.copy(deep=True)
            fill_ref = self.filling_data_ref(fill_ref, ref_cross)
            self.control_count_container(fill_ref, ref_cross)
            result.append(fill_ref)
        if full_cross:
            fill_full = dis_full.copy(deep=True)
            fill_full = self.filling_data_full_empty(fill_full, full_cross, 'full')
            self.control_count_container(fill_full, full_cross)
            result.append(fill_full)
        if empty_cross:
            fill_empty = dis_full.copy(deep=True)
            fill_empty = self.filling_data_full_empty(fill_empty, empty_cross, 'empty')
            self.control_count_container(fill_empty, empty_cross)
            result.append(fill_empty)
        self.finish_fill(result)


Missing().main()
