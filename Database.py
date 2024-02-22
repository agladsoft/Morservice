import datetime
import sys
from typing import List, Any

from __init__ import *

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))


class ClickHouse:

    def __init__(self):
        self.client: Client = self.connect_db()
        self.month, self.year, self.direction, self.start, self.terminal = self.get_month_year()

    @staticmethod
    def connect_db() -> Client:
        try:
            logger.info('Подключение к базе данных')
            client: Client = get_client(host='clickhouse', database='default',
                                        username="default", password="6QVnYsC4iSzz")
        except httpx.ConnectError as ex_connect:
            logger.info(f'Wrong connection {ex_connect}')
            sys.exit(1)
        return client

    @staticmethod
    def get_index(data_load: Optional[List[Tuple]]) -> Optional[int]:
        if data_load[0][4] == True and data_load[1][4] == True:
            return
        elif data_load[0][4] == True:
            return 0
        elif data_load[1][4] == True:
            return 1
        else:
            return

    def get_month_year(self) -> Optional[Tuple]:
        result: QueryResult = self.client.query(
            f"Select * from check_month")
        data_loaded: list = result.result_rows
        index = self.get_index(data_loaded)
        if index is None:
            logger.info('Не установлено значение is_on в True или оба терминала в True')
            sys.exit(1)
        month: int = data_loaded[index][1]
        year: int = data_loaded[index][2]
        direction: str = data_loaded[index][3]
        start: bool = data_loaded[index][4]
        terminal: str = data_loaded[index][5]
        # start = True
        if not start:
            logger.info('Не установлено значение is_on в True')
            sys.exit(1)
        return month, year, direction, start, terminal

    def get_table_in_db_positive(self, table: str, ref: bool = False) -> Optional[Tuple[int, DataFrame]]:
        'not_found_containers'
        'discrepancies_found_containers'
        if self.terminal == 'nmtp' and table == 'discrepancies_found_containers':
            return 0, DataFrame()
        terminal = self.terminal.upper()
        logger.info(f'Получение delta_count из представления {table}')
        result: QueryResult = self.client.query(
            f"Select * from {table} where delta_count > 0 and month = '{self.month}' and year = '{self.year}'"
            f" and direction = '{self.direction}' and stividor = '{terminal}'")
        data: Sequence = result.result_rows

        # Получаем список имен столбцов
        column_names: Sequence = result.column_names

        # Преобразуем результат в DataFrame
        df: DataFrame = pd.DataFrame(data, columns=column_names)
        if not ref and not df.empty:
            df: DataFrame = self.sort_params(df)
            return sum(df['delta_count'].to_list()), df
        if df.empty:
            return 0, df
        if ref:
            df: DataFrame = self.get_ref_line(df)
            return sum(df['delta_count'].to_list()), df

    @staticmethod
    def sort_params(df: DataFrame) -> DataFrame:
        if not df.get('line_unified').empty:
            df['line_unified']: DataFrame = df['line_unified'].str.upper().str.strip()
            filter_df: DataFrame = df.loc[~df['line_unified'].isin(PARAMETRS)]
            return filter_df
        df['operator']: DataFrame = df['operator'].str.upper().str.strip()
        filter_df: DataFrame = df.loc[~df['operator'].isin(PARAMETRS)]
        return filter_df

    @staticmethod
    def get_ref_line(df: DataFrame) -> DataFrame:
        if not df.get('line_unified').empty:
            df['line_unified']: DataFrame = df['line_unified'].str.upper().str.strip()
            filter_df: DataFrame = df.loc[df['line_unified'].isin(PARAMETRS)]
            return filter_df
        df['operator']: DataFrame = df['operator'].str.upper().str.strip()
        filter_df: DataFrame = df.loc[df['operator'].isin(PARAMETRS)]
        return filter_df

    def get_delta_teu(self, ref: bool, empty: bool) -> int:
        logger.info('Получение значения в delta_teo из nle_cross')
        if self.terminal == 'nle':
            terminal = 'НЛЭ'
        elif self.terminal == 'nmtp':
            terminal = 'НМТП'
        if empty:
            empty = 1
        else:
            empty = 0
        result: QueryResult = self.client.query(
            f"SELECT teu_delta FROM nle_cross where `month` = {self.month} and `year` = {self.year} and direction = '{self.direction}' and is_ref = {ref} and is_empty = {empty} and terminal = '{terminal}'")
        delta_teu: int = result.result_rows[0][0] if result.result_rows else 0
        return delta_teu if delta_teu is not None else 0

    @staticmethod
    def get_month_and_year(month: int, year: int) -> Optional[Tuple[int, int]]:
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
        return month, year

    def add_percent_in_df(self, df: DataFrame) -> DataFrame:
        summ_port = df['count'].sum()
        for index, row in df.iterrows():
            percent: float = round((row['count'] / summ_port) * 100)
            df.loc[index, 'percent'] = percent
        return self.filter_dataframe_to_percent(df)

    @staticmethod
    def filter_dataframe_to_percent(df: DataFrame) -> DataFrame:
        filter_df = df[df['percent'] >= 10]
        if len(filter_df) > 3:
            return filter_df.nlargest(3, 'percent')
        return filter_df

    def get_popular_port(self, ship_name: str) -> Optional[Tuple[DataFrame, int, int]]:
        logger.info('Получение информации о 3-х самых популярных портах')
        flag = True
        month = self.month
        year = self.year
        while flag:
            result: QueryResult = self.client.query(
                f"SELECT tracking_seaport,COUNT(tracking_seaport) as count "
                f"FROM {self.direction} where ship_name = '{ship_name}' "
                f"and month_parsed_on = {month} and year_parsed_on = {year}"
                f" GROUP by tracking_seaport,month_parsed_on,year_parsed_on ORDER BY count DESC")

            data: Sequence = result.result_rows

            # Получаем список имен столбцов
            column_names: Sequence = result.column_names

            # Преобразуем результат в DataFrame
            df: DataFrame = pd.DataFrame(data, columns=column_names)
            if not df.empty:
                df = self.add_percent_in_df(df)
                break
            if month == 1 and year == 2022:
                flag = False
                df = DataFrame()
                month = 1
                year = 1970
            else:
                month, year = self.get_month_and_year(month, year)

        return df, month, year

    def write_result(self, data_result):
        result = []
        if self.terminal == 'nle':
            for data in data_result:
                result.extend(self.write_to_table_nle(data))
        elif self.terminal == 'nmtp':
            for data in data_result:
                result.extend(self.write_to_table_nmtp(data))
        if result:
            self.client.insert('extrapolate', result,
                               column_names=['line', 'ship', 'direction', 'month', 'year', 'terminal', 'date',
                                             'container_type',
                                             'is_empty', 'is_ref', 'container_size',
                                             'count_container', 'goods_name', 'tracking_country', 'tracking_seaport',
                                             'month_port'])

    def write_to_table_nle(self, data_result: List[dict]) -> List[dict]:
        values = []
        for data in data_result:
            if not data.get('tracking_seaport'):
                continue
            line: str = data.get('line')
            ship: str = data.get('ship')
            direction: str = self.direction
            terminal: str = data.get('terminal')
            date: str = data.get('date')
            type_co: int = data.get('container_type')
            size: str = data.get('container_size')
            # count: int = data.get('count_container')
            is_empty: bool = data.get('is_empty')
            is_ref: bool = data.get('is_ref')
            goods_name: Optional[str] = 'ПОРОЖНИЙ КОНТЕЙНЕР' if is_empty else None
            month_port = f"{data.get('year_port')}.{data.get('month_port'):02}.01"
            for tracking_seaport, count in data.get('tracking_seaport').items():
                if count <= 0:
                    continue
                values.append(
                    [line, ship, direction, self.month, self.year, terminal, date, type_co, is_empty, is_ref, size,
                     count, goods_name,
                     self.get_tracking_country(tracking_seaport),
                     tracking_seaport, datetime.strptime(month_port, "%Y.%m.%d")])

        return values

    def write_to_table_nmtp(self, data_result: List[dict]) -> List[List[str | int | bool | Any]]:
        values = []
        for data in data_result:
            line: str = data.get('line')
            ship: str = data.get('ship')
            direction: str = self.direction
            terminal: str = data.get('terminal')
            date: str = data.get('date')
            type_co: int = data.get('container_type')
            size: str = data.get('container_size')
            count: int = data.get('count_container')
            is_empty: bool = data.get('is_empty')
            is_ref: bool = data.get('is_ref')
            goods_name: Optional[str] = 'ПОРОЖНИЙ КОНТЕЙНЕР' if is_empty else None
            if count <= 0:
                continue
            values.append(
                [line, ship, direction, self.month, self.year, terminal, date, type_co, is_empty, is_ref, size, count,
                 goods_name,
                 None, None, None])

        return values

    def get_reference_region(self) -> DataFrame:
        query: QueryResult = self.client.query('select * from reference_region')
        data: Sequence = query.result_rows
        column_names: Sequence = query.column_names
        df: DataFrame = pd.DataFrame(data, columns=column_names)
        return df

    def get_tracking_country(self, port_name: str) -> Optional[str]:
        reference_region: DataFrame = self.get_reference_region()
        query: Series = (reference_region['seaport_unified'] == port_name)
        country: List[str] = list(set(reference_region.loc[query, 'country'].to_list()))
        tracking_country: str = country[0] if country else None
        return tracking_country
