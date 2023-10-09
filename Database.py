from typing import Tuple

from clickhouse_connect.driver.query import QueryResult
from pandas import DataFrame

from __init__ import *

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))


class ClickHouse:

    def __init__(self):
        self.client: Client = self.connect_db()
        self.month, self.year, self.direction, self.is_ref, self.start = self.get_month_year()

    @staticmethod
    def connect_db() -> Client:
        try:
            logger.info('Подключение к базе данных')
            client: Client = get_client(host='clickhouse', database='default',
                                        username="admin", password="6QVnYsC4iSzz")
        except httpx.ConnectError as ex_connect:
            logger.info(f'Wrong connection {ex_connect}')
            sys.exit(1)
        return client

    def get_month_year(self) -> Optional[Tuple]:
        result: QueryResult = self.client.query(
            f"Select * from check_month")
        data_loaded: list = result.result_rows
        month: int = data_loaded[0][1]
        year: int = data_loaded[0][2]
        direction: str = data_loaded[0][3]
        is_ref: bool = data_loaded[0][4]
        start: bool = data_loaded[0][5]

        return month, year, direction, is_ref, start

    def get_table_in_db_positive(self, table: str, ref: bool = False) -> Optional[Tuple[int, DataFrame]]:
        'not_found_containers'
        'discrepancies_found_containers'

        logger.info(f'Получение delta_count из представления {table}')
        result: QueryResult = self.client.query(
            f"Select * from {table} where delta_count > 0 and month = '{self.month}' and year = '{self.year}' and direction = '{self.direction}'")
        data: list = result.result_rows

        # Получаем список имен столбцов
        column_names: list = result.column_names

        # Преобразуем результат в DataFrame
        df: DataFrame = pd.DataFrame(data, columns=column_names)
        if not ref and not df.empty:
            df: DataFrame = self.sort_params(df)
        if df.empty:
            return 0, df
        return sum(df['delta_count'].to_list()), df

    @staticmethod
    def sort_params(df: DataFrame) -> DataFrame:
        df['operator']: DataFrame = df['operator'].str.upper().str.strip()
        filter_df: DataFrame = df.loc[~df['operator'].isin(PARAMETRS)]
        return filter_df

    def get_delta_teu(self) -> int:
        logger.info('Получение значения в delta_teo из nle_cross')
        result: QueryResult = self.client.query(
            f"SELECT teu_delta FROM nle_cross where `month` = {self.month} and `year` = {self.year} and direction = '{self.direction}' and is_ref = {self.is_ref} and is_empty = 0")
        delta_teu: int = result.result_rows[0][0] if result.result_rows else 0
        return delta_teu

    def write_result(self, data_result):
        for data in data_result:
            self.write_to_table(data)

    def write_to_table(self, data_result: List[dict]) -> None:
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
            if count <= 0:
                continue
            values.append(
                [line, ship, direction, terminal, date, type_co, size, count, None, None, None])
        if values:
            self.client.insert('extrapolate', values,
                               column_names=['line', 'ship', 'direction', 'terminal', 'date', 'container_type',
                                             'container_size',
                                             'count_container', 'goods_name', 'tracking_country', 'tracking_seaport'])
