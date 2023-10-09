from __init__ import *
from Database import ClickHouse

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))


class Morservice:

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
        return data_di.get('atb_moor_pier') if data_di.get('shipment_date') is None else data_di.get('shipment_date')

    @staticmethod
    def add_container(data: dict, count: float, flag: bool) -> None:
        if flag:
            data['container_type'] = 'HC'
            data['container_size'] = 40
        else:
            data['container_type'] = 'DC'
            data['container_size'] = 20
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

    def add_columns(self, data_df: Series, percent40: float) -> Optional[list]:
        logger.info('Заполнение данных по контейнерам между 40фт и 20 фт в процентном соотношение')
        delta_count: int = data_df['delta_count']
        data_result: list = []
        if delta_count <= 0:
            return None
        feet_40: int = round((delta_count * percent40) / 100)
        feet_20: int = delta_count - feet_40
        # Распределение данных по контейнерам 40 футовым
        data: dict = self.get_data(data_df)
        self.add_container(data, feet_40, True)
        data_result.append(data)
        # Распределение данных по контейнерам 20 футовым
        data: dict = self.get_data(data_df)
        self.add_container(data, feet_20, False)
        data_result.append(data)
        return data_result

    def get_data(self, data_df: Union[dict, Series]) -> dict:
        data = {
            'line': data_df['operator'],
            'ship': data_df['ship_name_unified'],
            'terminal': 'НЛЭ',
            'date': self.get_date(data_df)
        }
        return data

    def filling_in_data(self, percent: float, df: DataFrame) -> list:
        logger.info('Заполнение данных в таблицу')
        data_list = []
        for index, d in df.iterrows():
            data_result = self.add_columns(d, percent)
            if data_result is None:
                continue
            data_list.append(data_result)
        return data_list

    def filling_in_data_no(self, df: DataFrame, delta_teu: float) -> list:
        data_list = []
        count_container_40: float = (delta_teu // 2) // 2
        count_container_20: float = count_container_40 * 2
        for index, row in df.iterrows():
            lst: list = []
            data: dict = self.get_data(row)
            feet_40: float = round((count_container_40 * row['percent']) / 100)
            self.add_container(data, feet_40, True)
            lst.append(data)
            data: dict = self.get_data(row)
            feet_20: float = round((count_container_20 * row['percent']) / 100)
            self.add_container(data, feet_20, False)
            lst.append(data)
            data_list.append(lst)
        return data_list

    def data_no_is_not_empty(self, delta_teu: float, data_no_count: float, data_no: DataFrame, data_dis_count: float,
                             data_dis: DataFrame) -> Optional[list]:
        percent40_not: float = self.not_percentage(delta_teu, data_no_count)
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

    def main(self):
        if self.clickhouse.start:
            delta_teu: float = self.clickhouse.get_delta_teu()
            data_result: Union[bool, list] = False
            if delta_teu > 0:
                data_no_count: float
                data_no: DataFrame
                data_dis_count: float
                data_dis: DataFrame
                data_no_count, data_no = self.clickhouse.get_table_in_db_positive('not_found_containers')
                data_dis_count, data_dis = self.clickhouse.get_table_in_db_positive('discrepancies_found_containers')
                if not data_no.empty:
                    data_result: list = self.data_no_is_not_empty(delta_teu, data_no_count, data_no, data_dis_count,
                                                                  data_dis)
                elif delta_teu > 0 and not data_dis.empty:
                    data_result: list = self.data_no_is_empty(delta_teu, data_dis, data_dis_count)
                if data_result:
                    self.clickhouse.write_result(data_result)


if __name__ == '__main__':
    Morservice().main()
