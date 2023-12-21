import numpy as np

from Database import ClickHouse
from __init__ import *

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))


class Import_and_Export:

    def __init__(self):
        self.clickhouse: ClickHouse = ClickHouse()

    @staticmethod
    def not_percentage(delta_teu: float, data_count: float) -> float:
        logger.info('Получение процентного соотношения 40 футовых и 20 футовых контейнеров')
        percent40: float = ((float(delta_teu) / float(data_count)) - 1) * 100
        return percent40

    @staticmethod
    def get_index(data_result: list) -> Optional[int]:
        max_index: None = None
        max_value: int = 0
        for index, value in enumerate(data_result):
            for d in value:
                if d.get('count_container', 0) > max_value:
                    max_index: int = index
                    max_value: int = d.get('count_container')
        return max_index

    @staticmethod
    def distribution_teu(df: DataFrame, data_count: float) -> DataFrame:
        for index, row in df.iterrows():
            percent: float = round((row['delta_count'] / data_count) * 100)
            df.loc[index, 'percent'] = percent
        return df

    @staticmethod
    def get_date(data_di: dict) -> Optional[str]:
        if 'shipment_date' not in data_di:
            return data_di.get('atb_moor_pier')

        else:
            if type(data_di.get('shipment_date')) in (float,):
                return data_di.get('atb_moor_pier')
            else:
                return data_di.get('atb_moor_pier') if data_di.get('shipment_date') is None else data_di.get(
                    'shipment_date')

    @staticmethod
    def add_container(data: dict, count: float, flag: bool, ref: bool = False) -> None:
        if flag:
            data['container_type'] = 'HC' if not ref else 'REF'
            data['container_size'] = 40

        else:
            data['container_type'] = 'DC' if not ref else 'REF'
            data['container_size'] = 20

        data['is_ref'] = data.get('container_type') == 'REF'
        data['count_container'] = count

    @staticmethod
    def check_enough_teu(delta_teu: float, data_count: float) -> bool:
        feet_40: float = (delta_teu // 2) // 2
        feet_20: float = feet_40 * 2
        sum_ft: float = feet_20 + feet_40
        if sum_ft >= data_count:
            return True
        return False

    @staticmethod
    def get_sum_delta_teu(data_result: list) -> int:
        delta_teu: int = 0
        for data in data_result:
            delta_teu += data[0]['count_container'] * 2
            delta_teu += data[1]['count_container']
        return delta_teu

    @staticmethod
    def sum_delta_count(data: list) -> int:
        '''Подсчёт полученной суммы delta_teo'''
        summa: int = 0
        for d in data:
            for i in d:
                if i['container_size'] == 40:
                    summa += 2 * i['count_container']
                else:
                    summa += 1 * i['count_container']
        return summa

    @staticmethod
    def get_index_df(data_dis, date, line, ship):
        if not data_dis.loc[(data_dis['atb_moor_pier'] == date) & (data_dis['operator'] == line) & (
                data_dis['ship_name_unified'] == ship)].index.empty:
            return data_dis.loc[(data_dis['atb_moor_pier'] == date) & (data_dis['operator'] == line) & (
                    data_dis['ship_name_unified'] == ship)].index[0]
        return False

    def get_different_df(self, data_dis, data_result_dis):
        for data in data_result_dis:
            line = data[0].get('line')
            ship = data[0].get('ship')
            date = data[0].get('date')
            count = sum([data[0].get('count_container'), data[1].get('count_container')])
            index = self.get_index_df(data_dis, date, line, ship)
            if type(index) in (bool,) and not index:
                continue
            else:
                data_dis.at[index, 'delta_count'] -= count
        return data_dis

    def change_20(self, data_result: list, different: int) -> list:
        '''Изменение 20 футового на 40 футовый контейнер'''
        index: int = self.get_index(data_result)
        data_result[index][-1]['count_container'] += abs(different)
        data_result[index][0]['count_container'] -= abs(different)
        return data_result

    def change_40(self, data_result: list, different: int) -> list:
        '''Изменение 40 футового на 20 футовый '''
        index: int = self.get_index(data_result)
        data_result[index][0]['count_container'] += abs(different)
        data_result[index][-1]['count_container'] -= abs(different)
        return data_result

    def check_delta_teu(self, data_result: list, delta_teu: float) -> list:
        '''Сверка delta_teu с заполненным результатом и приравнивание к 0'''
        summa_result: int = self.sum_delta_count(data_result)
        different: int = int(delta_teu) - summa_result
        if type(self) is Ref:
            if different == 1:
                data_result[0][1]['count_container'] += different
            elif different == -1:
                data_result = self.change_20(data_result, different)

            if self.sum_delta_count(data_result) == delta_teu:
                return data_result

        else:
            if summa_result != int(delta_teu):
                if different > 0:
                    data_result: list = self.change_40(data_result, different)
                    if self.sum_delta_count(data_result) == delta_teu:
                        return data_result
                elif different < 0:
                    data_result: list = self.change_20(data_result, different)
                    if self.sum_delta_count(data_result) == delta_teu:
                        return data_result
                    else:
                        self.check_delta_teu(data_result, delta_teu)

            else:
                return data_result

    def add_columns(self, data_df: Series, percent40: float, flag_ref=False) -> Optional[list]:
        logger.info('Заполнение данных по контейнерам между 40фт и 20 фт в процентном соотношение')
        delta_count: int = data_df['delta_count']
        data_result: list = []
        if delta_count <= 0:
            return None
        if type(self) is Ref or flag_ref:
            feet_40: int = round((delta_count * percent40) / 100)
            feet_20: int = delta_count - feet_40
        else:
            feet_40: int = round((delta_count * percent40) / 100)
            feet_20: int = delta_count - feet_40
        # Распределение данных по контейнерам 40 футовым
        data: dict = self.get_data(data_df)
        if type(self) is Ref or flag_ref:
            self.add_container(data, feet_40, True, ref=True)
        else:
            self.add_container(data, feet_40, True)
        data_result.append(data)
        # Распределение данных по контейнерам 20 футовым
        data: dict = self.get_data(data_df)
        if type(self) is Ref or flag_ref:
            self.add_container(data, feet_20, False, ref=True)
        else:
            self.add_container(data, feet_20, False)
        data_result.append(data)
        return data_result

    def get_data(self, data_df: Union[dict, Series]) -> dict:
        data = {
            'line': data_df['operator'],
            'ship': data_df['ship_name_unified'],
            'terminal': 'НЛЭ',
            'date': self.get_date(data_df),
            'is_empty': type(self) is Empty and not type(self) is Ref

        }
        return data

    def filling_in_data(self, percent: float, df: DataFrame, flag_ref=False) -> list:
        logger.info('Заполнение данных в таблицу')
        data_list = []
        for index, d in df.iterrows():
            data_result = self.add_columns(d, percent, flag_ref)
            if data_result is None:
                continue
            data_list.append(data_result)
        return data_list

    def filling_in_data_no(self, df: DataFrame, delta_teu: float) -> list:
        data_list: list = []
        if type(self) is Ref:
            count_container_40: float = delta_teu // 2
            count_container_20: float = 0 if delta_teu % 2 == 0 else 1
        else:
            count_container_40: float = (delta_teu // 2) // 2
            count_container_20: float = count_container_40 * 2
        for index, row in df.iterrows():
            lst: list = []
            data: dict = self.get_data(row)
            feet_40: float = round((count_container_40 * row['percent']) / 100)
            if type(self) is Ref:
                self.add_container(data, feet_40, True, ref=True)
            else:
                self.add_container(data, feet_40, True)
            lst.append(data)
            data: dict = self.get_data(row)
            feet_20: float = round((count_container_20 * row['percent']) / 100)
            if type(self) is Ref:
                self.add_container(data, feet_20, False, ref=True)
            else:
                self.add_container(data, feet_20, False)
            lst.append(data)
            data_list.append(lst)
        return data_list

    def data_no_is_not_empty(self, delta_teu: float, data_no_count: float, data_no: DataFrame, data_dis_count: float,
                             data_dis: DataFrame) -> Optional[list]:
        percent40_not: float = round(self.not_percentage(delta_teu, data_no_count))
        data_result: Union[None, list] = None
        if 45 <= percent40_not <= 55:
            data_result: list = self.filling_in_data(percent40_not, data_no)
            data_result: list = self.check_delta_teu(data_result, delta_teu)
        elif percent40_not <= 0:
            data_no: DataFrame = self.distribution_teu(data_no, data_no_count)
            data_result: list = self.filling_in_data_no(data_no, delta_teu)
            data_result: list = self.check_delta_teu(data_result, delta_teu)
        elif percent40_not > 0:

            if self.check_enough_teu(delta_teu, data_no_count):
                if percent40_not >= 100 and data_dis_count <= 0:
                    data_result: list = self.filling_in_data(100, data_no)
                    data_result: list = self.check_delta_teu(data_result, delta_teu)
                    return data_result
                data_result_no: list = self.filling_in_data(50, data_no)
                delta_teu -= self.get_sum_delta_teu(data_result_no)
                percent40_dis: float = self.not_percentage(delta_teu, data_dis_count)
                if percent40_dis >= 100:
                    data_result_dis: list = self.filling_in_data(100, data_dis)
                    data_result: list = data_result_no + data_result_dis
                elif percent40_dis > 0:
                    data_result_dis: list = self.filling_in_data(percent40_dis, data_dis)
                    data_result_dis: list = self.check_delta_teu(data_result_dis, delta_teu)
                    data_result: list = data_result_no + data_result_dis
                elif percent40_dis <= 0:
                    data_dis: DataFrame = self.distribution_teu(data_dis, data_dis_count)
                    data_result_dis: list = self.filling_in_data_no(data_dis, delta_teu)
                    data_result_dis: list = self.check_delta_teu(data_result_dis, delta_teu)
                    data_result: list = data_result_no + data_result_dis
            else:
                data_result: list = self.filling_in_data(percent40_not, data_no)
                data_result: list = self.check_delta_teu(data_result, delta_teu)

        return data_result

    def data_no_is_empty(self, delta_teu: float, data_dis: DataFrame, data_di_count: float) -> Optional[list]:
        percent40_dis: float = self.not_percentage(delta_teu, data_di_count)
        data_result_dis: Union[list, None] = None
        if percent40_dis >= 100:
            data_result_dis: list = self.filling_in_data(100, data_dis)
        elif percent40_dis > 0:
            data_result_dis: list = self.filling_in_data(percent40_dis, data_dis)
            data_result_dis: list = self.check_delta_teu(data_result_dis, delta_teu)
        elif percent40_dis <= 0:
            data_dis: DataFrame = self.distribution_teu(data_dis, data_di_count)
            data_result_dis: list = self.filling_in_data_no(data_dis, delta_teu)
            data_result_dis: list = self.check_delta_teu(data_result_dis, delta_teu)
        return data_result_dis

    @staticmethod
    def get_diff_Dataframe(data_dis, df):
        if isinstance(df, DataFrame):
            diff_df = data_dis.copy()

            # Вычитаем разницу из столбца "delta_count"
            diff_df['delta_count'] = data_dis['delta_count'] - df['delta_count']

            return diff_df.query('delta_count != 0')
        return

    def main(self, df):
        df, flag_not = df
        delta_teu: float = self.clickhouse.get_delta_teu(ref=False, empty=False)
        data_result: Union[bool, list] = False
        if delta_teu <= 0:
            return None, None
        data_no_count: float
        data_no: DataFrame
        data_dis_count: float
        data_dis: DataFrame
        data_no_count, data_no = self.clickhouse.get_table_in_db_positive('not_found_containers')
        data_dis_count, data_dis = self.clickhouse.get_table_in_db_positive('discrepancies_found_containers')
        diff: Optional[DataFrame] = None
        if isinstance(df, DataFrame) and not data_dis.equals(df) and flag_not == 'dis':
            diff = self.get_diff_Dataframe(data_dis, df)
            data_dis = df
            data_dis_count = sum(data_dis['delta_count'].to_list())
        elif isinstance(df, DataFrame) and not data_no.equals(df) and flag_not == 'not':
            diff = self.get_diff_Dataframe(data_no, df)
            data_no = diff
            data_no_count = sum(data_no['delta_count'].to_list())
            diff = pd.concat([df, data_dis])
            data_dis = DataFrame()
            data_dis_count = 0
        elif isinstance(df, DataFrame) and flag_not == 'all':
            diff = self.get_diff_Dataframe(pd.concat([data_no, data_dis], ignore_index=True), df)
            data_dis = diff
            data_dis_count = sum(data_dis['delta_count'].to_list())
            diff = df
            data_no = DataFrame()
            data_no_count = 0
        if not data_no.empty:
            data_result: list = self.data_no_is_not_empty(delta_teu, data_no_count, data_no, data_dis_count,
                                                          data_dis)
        elif delta_teu > 0 and not data_dis.empty:
            data_result: list = self.data_no_is_empty(delta_teu, data_dis, data_dis_count)

        if df is None and not flag_not and data_dis_count > 0:
            diff = self.get_different_df(data_dis, data_result)
        return diff, data_result


class Ref(Import_and_Export):
    def __init__(self):
        super().__init__()
        self.df_difference = None

    @staticmethod
    def change_df(df, diff):
        while diff > 0:
            summ_count = sum(df['delta_count'].to_list())
            for index, d in df.sort_values(by=['delta_count'], ascending=False).iterrows():
                if diff <= d['delta_count']:
                    df.at[index, 'delta_count'] -= diff
                    return df
                else:
                    percent = d.get('delta_count') / summ_count
                    df.at[index, 'delta_count'] -= round(diff * percent)
                    diff -= round(diff * percent)
        return df

    def get_different_df(self, data_dis, data_result_dis):
        for data in data_result_dis:
            line = data[0].get('line')
            ship = data[0].get('ship')
            date = data[0].get('date')
            count = sum([data[0].get('count_container'), data[1].get('count_container')])
            index = data_dis.loc[(data_dis['atb_moor_pier'] == date) & (data_dis['operator'] == line) & (
                    data_dis['ship_name_unified'] == ship)].index[0]
            data_dis.at[index, 'delta_count'] -= count
        self.df_difference = data_dis

    def data_no_is_empty_ref(self, data_dis, data_dis_count, delta_teu, flag_ref=False):
        container_40_ft = delta_teu // 2 if delta_teu % 2 == 0 else (delta_teu // 2) + 1
        if container_40_ft >= data_dis_count:
            data_result_dis = self.filling_in_data(100, data_dis, flag_ref)
            # data_result_dis = self.check_delta_teu(data_result_dis, delta_teu)
            return data_result_dis
        elif container_40_ft < data_dis_count:
            diff = data_dis_count - container_40_ft
            data_ref_copy = data_dis.copy()
            data_ref_dis = self.change_df(data_dis, diff)
            data_result_dis = self.filling_in_data(100, data_ref_dis, flag_ref)
            data_result_dis = self.check_delta_teu(data_result_dis, delta_teu)
            self.get_different_df(data_ref_copy, data_result_dis)
            return data_result_dis

    def data_no_is_not_empty_ref(self, data_ref_no, data_ref_dis, data_no_count, data_dis_count, delta_teu):
        data_result = None
        container_40_ft = delta_teu // 2 if delta_teu % 2 == 0 else (delta_teu // 2) + 1
        if container_40_ft >= data_no_count:
            data_result_no = self.filling_in_data(100, data_ref_no)
            delta_teu -= (data_no_count * 2)
            container_40_ft = delta_teu // 2 if delta_teu % 2 == 0 else (delta_teu // 2) + 1
            if container_40_ft >= data_dis_count:
                data_result_dis = self.filling_in_data(100, data_ref_dis)
                # data_result_dis = self.check_delta_teu(data_result_dis, delta_teu)
                data_result = data_result_no + data_result_dis
                return data_result
            elif container_40_ft < data_dis_count:
                diff = data_dis_count - container_40_ft
                data_ref_copy = data_ref_dis.copy()
                data_ref_dis = self.change_df(data_ref_dis, diff)
                data_result_dis = self.filling_in_data(100, data_ref_dis)
                data_result_dis = self.check_delta_teu(data_result_dis, delta_teu)
                data_result = data_result_no + data_result_dis
                self.get_different_df(data_ref_copy, data_result_dis)
                return data_result

        elif container_40_ft < data_no_count:
            diff = data_no_count - container_40_ft
            data_ref_copy = data_ref_no.copy()
            data_ref_no = self.change_df(data_ref_no, diff)
            data_result_no = self.filling_in_data(100, data_ref_no)
            data_result_no = self.check_delta_teu(data_result_no, delta_teu)
            self.get_different_df(data_ref_copy, data_result_no)
            return data_result_no

        return data_result

    def main(self):
        delta_teu: float = self.clickhouse.get_delta_teu(ref=True, empty=False)
        data_result: Optional[list] = None
        if delta_teu <= 0:
            return data_result
        data_ref_no_count: int
        data_ref_no: DataFrame
        data_ref_dis_count: int
        data_ref_dis: DataFrame
        data_ref_no_count, data_ref_no = self.clickhouse.get_table_in_db_positive(
            'not_found_containers', ref=True)
        data_ref_dis_count, data_ref_dis = self.clickhouse.get_table_in_db_positive(
            'discrepancies_found_containers',
            ref=True)
        if not data_ref_no.empty:
            data_result = self.data_no_is_not_empty_ref(data_ref_no, data_ref_dis, data_ref_no_count,
                                                        data_ref_dis_count, delta_teu)

        elif not data_ref_dis.empty:
            data_result = self.data_no_is_empty_ref(data_ref_dis, data_ref_dis_count, delta_teu)

        return data_result


class Empty(Ref, Import_and_Export):

    def __init__(self):
        super().__init__()
        self.delta_teu = None
        self.result_empty = None

    def preliminary_processing(self, df: DataFrame):
        delta_teu: float = self.clickhouse.get_delta_teu(ref=False, empty=True)
        if isinstance(df, DataFrame) and not df.empty:
            df = df[df.delta_count > 0]
            count_container = sum(df['delta_count'])
            data_result_empty = self.data_no_is_empty_ref(df, count_container, delta_teu, flag_ref=True)
            delta_teu -= self.sum_delta_count(data_result_empty)
            self.result_empty, self.delta_teu = data_result_empty, delta_teu
        else:
            self.result_empty, self.delta_teu = [], delta_teu

    def start(self, df: DataFrame):
        data_result: Optional[DataFrame] = None
        if self.delta_teu <= 0:
            if not self.result_empty:
                return data_result
            return self.result_empty
        elif isinstance(df, DataFrame) and not df.empty:
            df_count = sum(df['delta_count'].to_list())
            data_result = self.data_no_is_empty(self.delta_teu, df, df_count)

        if df is None and self.clickhouse.get_delta_teu(ref=False, empty=False) < 0:
            data_no_count, data_no = self.clickhouse.get_table_in_db_positive(
                'not_found_containers')
            data_dis_count, data_dis = self.clickhouse.get_table_in_db_positive(
                'discrepancies_found_containers')
            if not data_no.empty:
                data_result = self.data_no_is_not_empty(self.delta_teu, data_no_count, data_no,
                                                        data_dis_count, data_dis)

            elif not data_dis.empty:
                data_result = self.data_no_is_empty(self.delta_teu, data_dis, data_dis_count)

        # elif df is None:
        # data_no_count, data_no = self.clickhouse.get_table_in_db_positive(
        #     'not_found_containers')
        # data_dis_count, data_dis = self.clickhouse.get_table_in_db_positive(
        #     'discrepancies_found_containers')
        # if not data_no.empty:
        #     data_result = self.data_no_is_not_empty(self.delta_teu, data_no_count, data_no,
        #                                             data_dis_count, data_dis)
        #
        # elif not data_dis.empty:
        #     data_result = self.data_no_is_empty(self.delta_teu, data_dis, data_dis_count)

        if self.result_empty:
            data_result = self.result_empty if data_result is None else data_result + self.result_empty
        return data_result


class Extrapolate:

    def __init__(self):
        self.ref = Ref()
        self.empty = Empty()
        self.import_end_export = Import_and_Export()

    @staticmethod
    def sample_difference_from(sum_container, df):
        while sum_container > 0:
            for index, d in df.sort_values(by=['delta_count'], ascending=False).iterrows():
                if sum_container <= d['delta_count']:
                    df.at[index, 'delta_count'] -= sum_container
                    return df
                else:
                    percent = d.get('delta_count') / sum_container
                    df.at[index, 'delta_count'] -= round(sum_container * percent)
                    sum_container -= round(sum_container * percent)
        return df


    def check_enough_container(self):
        delta_teu_empty: float = self.empty.delta_teu
        delta_teu_imp_and_exp: float = self.import_end_export.clickhouse.get_delta_teu(ref=False, empty=False)
        if delta_teu_imp_and_exp <= 0:
            return None, False
        elif delta_teu_empty <= 0:
            return None, False
        sum_delta_teu: float = sum([i for i in [delta_teu_empty, delta_teu_imp_and_exp] if i > 0])
        dis_count, dis_df = self.import_end_export.clickhouse.get_table_in_db_positive('discrepancies_found_containers')
        not_count, not_df = self.import_end_export.clickhouse.get_table_in_db_positive('not_found_containers')
        if sum([dis_count, not_count]) <= 0:
            return None, False
        percent: float = ((sum_delta_teu / sum([dis_count, not_count])) - 1) * 100

        if 0 < percent <= 100:
            percent: float = ((sum([dis_count, not_count]) / sum_delta_teu)) * 100
            total_number_of_containers = round(delta_teu_empty * (percent / 100))
            container_empty_40ft: float = abs(total_number_of_containers - delta_teu_empty)
            container_empty_20ft: float = total_number_of_containers - container_empty_40ft
            if dis_count > sum([container_empty_20ft, container_empty_40ft]):
                dis_df = self.sample_difference_from(sum([container_empty_20ft, container_empty_40ft]), dis_df)
                return dis_df, 'dis'
            # elif not_count > sum([container_empty_20ft, container_empty_40ft]):
            #     ...

            else:
                container_empty_40ft: float = round((delta_teu_imp_and_exp // 2) * (percent / 100))
                container_empty_20ft: float = delta_teu_imp_and_exp - (container_empty_40ft * 2)
                union_df = self.sample_difference_from(sum([container_empty_20ft, container_empty_40ft]),
                                                       pd.concat([not_df, dis_df], ignore_index=True))
                return union_df, 'all'


        elif percent < 0:
            return None, False
        elif percent > 100:
            sufficient_number_of_containers = (delta_teu_imp_and_exp // 2) if delta_teu_imp_and_exp % 2 == 0 \
                else (delta_teu_imp_and_exp // 2) + 1
            if not_count >= sufficient_number_of_containers:
                not_df = self.sample_difference_from(sufficient_number_of_containers, not_df)
                return not_df, 'not'

            elif dis_count >= sufficient_number_of_containers:
                sufficient_number_of_containers = (delta_teu_imp_and_exp // 2) if delta_teu_imp_and_exp % 2 == 0 \
                    else (delta_teu_imp_and_exp // 2) + 1
                diff_count = sufficient_number_of_containers - not_count
                dis_df = self.sample_difference_from(diff_count, dis_df)
                return dis_df, 'dis'

            elif sum([not_count, dis_count]) >= sufficient_number_of_containers:
                container_empty_40ft: float = round((delta_teu_imp_and_exp // 2) * (percent / 100))
                container_empty_20ft: float = delta_teu_imp_and_exp - (container_empty_40ft * 2)
                union_df = self.sample_difference_from(sum([container_empty_20ft, container_empty_40ft]),
                                                       pd.concat([not_df, dis_df], ignore_index=True))
                return union_df, 'all'
            else:
                return None, True

    @staticmethod
    def add_month_year(line_tuple: List[dict], month: int, year: int) -> None:
        for line in line_tuple:
            line.update({'month_port': month, 'year_port': year})

    def distribution_of_containers_by_ports(self, data: Dict, df: DataFrame):
        if data.get('count_container') <= 2:
            return {df.get('tracking_seaport').to_list()[0]: data.get('count_container')}
        if df.empty:
            return
        data_port = self.filling_count_to_percent(data, df)
        data_port = {k: v for k, v in data_port.items() if v > 0}
        return data_port

    def filling_count_to_percent(self, data: Dict, df: DataFrame) -> Dict:
        data_port = {}
        if df.empty:
            return {'tracking_seaport': None}
        for index, item in df[::-1].iterrows():
            data_port[item['tracking_seaport']] = int(round((item['percent'] / 100) * data.get('count_container')))
        if sum(list(data_port.values())) != data.get('count_container'):
            data_port = self.filling_in_missing_data(data_port, data)
        return data_port


    def filling_in_missing_data(self,data_port: Dict, data: Dict):
        summ_result = sum(list(data_port.values()))
        diff = data.get('count_container') - summ_result
        max_port = sorted(data_port.items(), key=lambda x: -x[1])[0][0]
        data_port[max_port] += diff
        if sum(list(data_port.values())) == data.get('count_container'):
            return data_port
        return self.filling_in_missing_data(data_port,data)


    @staticmethod
    def get_information_port(lst_data: List[dict]):
        return lst_data[0].get('ship')

    def fill_line(self, list_data: List[dict], df: DataFrame) -> None:
        for line in list_data:
            if line.get('count_container') <= 0:
                continue
            line.setdefault('tracking_seaport', self.distribution_of_containers_by_ports(line, df))

    def add_port_in_line(self, lst_result: List[List[dict]]) -> List[List[dict]]:
        for line in lst_result:
            port: str = self.get_information_port(line)
            df_port: DataFrame
            month: int
            year: int
            df_port, month, year = self.import_end_export.clickhouse.get_popular_port(port)
            self.add_month_year(line, month, year)
            self.fill_line(line, df_port)
        return lst_result

    def main(self):
        result_ref = self.ref.main()
        self.empty.preliminary_processing(self.ref.df_difference)
        dis_df = self.check_enough_container()
        diff, result_imp_and_exp = self.import_end_export.main(dis_df)
        result_empty = self.empty.start(diff)
        result = []
        for i in [result_ref, result_empty, result_imp_and_exp]:
            if i:
                result += i
        result_port = self.add_port_in_line(result)
        self.import_end_export.clickhouse.write_result(result_port)



if __name__ == '__main__':
    logger.info('Start Working')
    Extrapolate().main()
    logger.info('End Working')
