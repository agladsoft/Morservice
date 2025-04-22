import sys

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import math
from pandas import DataFrame, Series

# Импортируем тестируемые классы
# Предполагаем, что модуль расположен в src.scripts.Import_and_Export
sys.modules['src.scripts.app_logger'] = MagicMock()
from src.scripts.Ref import Import_and_Export, Ref, Empty, Extrapolate


# Фикстура для создания экземпляра класса с моком ClickHouse
@pytest.fixture
def import_export():
    with patch('src.scripts.Ref.ClickHouse') as mock_clickhouse:
        instance = Import_and_Export()
        instance.clickhouse = mock_clickhouse
        yield instance


@pytest.fixture
def ref_class():
    with patch('src.scripts.Ref.ClickHouse') as mock_clickhouse:
        instance = Ref()
        instance.clickhouse = mock_clickhouse
        yield instance


@pytest.fixture
def empty_class():
    with patch('src.scripts.Ref.ClickHouse') as mock_clickhouse:
        instance = Empty()
        instance.clickhouse = mock_clickhouse
        instance.delta_teu = 10  # Устанавливаем значение для тестов
        instance.result_empty = None
        yield instance


@pytest.fixture
def extrapolate_class():
    with patch('src.scripts.Ref.ClickHouse') as mock_clickhouse:
        instance = Extrapolate()
        # Мокаем вложенные классы
        instance.ref = Mock()
        instance.empty = Mock()
        instance.import_end_export = Mock()
        instance.import_end_export.clickhouse = mock_clickhouse
        yield instance


# Тесты для статических методов класса Import_and_Export
def test_not_percentage():
    # Тест расчета процентного соотношения
    result = Import_and_Export.not_percentage(150, 100)
    assert result == 50.0

    result = Import_and_Export.not_percentage(100, 100)
    assert result == 0.0


def test_get_index():
    # Тест поиска индекса максимального значения
    data = [
        [{'count_container': 5}],
        [{'count_container': 10}],
        [{'count_container': 3}]
    ]
    assert Import_and_Export.get_index(data) == 1

    # Пустой список
    assert Import_and_Export.get_index([]) is None


def test_distribution_teu():
    # Тест распределения TEU
    df = pd.DataFrame({
        'delta_count': [100, 200, 300]
    })
    data_count = 600

    result_df = Import_and_Export.distribution_teu(df, data_count)

    # Проверяем процентные значения
    assert result_df['percent'].tolist() == [17, 33, 50]  # округленные значения


def test_get_date():
    # Тест получения даты из словаря
    # Случай когда shipment_date отсутствует
    data_di = {'atb_moor_pier': '2023-01-01'}
    assert Import_and_Export.get_date(data_di) == '2023-01-01'

    # Случай когда shipment_date - число
    data_di = {'shipment_date': 10.5, 'atb_moor_pier': '2023-01-01'}
    assert Import_and_Export.get_date(data_di) == '2023-01-01'

    # Случай когда shipment_date - None
    data_di = {'shipment_date': None, 'atb_moor_pier': '2023-01-01'}
    assert Import_and_Export.get_date(data_di) == '2023-01-01'

    # Случай когда shipment_date присутствует и валидно
    data_di = {'shipment_date': '2023-02-01', 'atb_moor_pier': '2023-01-01'}
    assert Import_and_Export.get_date(data_di) == '2023-02-01'


def test_add_container():
    # Тест добавления контейнера
    data = {}

    # 40 футовый контейнер не рефрижератор
    Import_and_Export.add_container(data, 5, True)
    assert data == {
        'container_type': 'HC',
        'container_size': 40,
        'is_ref': False,
        'count_container': 5
    }

    # 20 футовый контейнер не рефрижератор
    data = {}
    Import_and_Export.add_container(data, 3, False)
    assert data == {
        'container_type': 'DC',
        'container_size': 20,
        'is_ref': False,
        'count_container': 3
    }

    # 40 футовый рефрижератор
    data = {}
    Import_and_Export.add_container(data, 2, True, True)
    assert data == {
        'container_type': 'REF',
        'container_size': 40,
        'is_ref': True,
        'count_container': 2
    }

    # 20 футовый рефрижератор
    data = {}
    Import_and_Export.add_container(data, 4, False, True)
    assert data == {
        'container_type': 'REF',
        'container_size': 20,
        'is_ref': True,
        'count_container': 4
    }


def test_check_enough_teu():
    # Тест проверки достаточности TEU
    # Достаточно
    assert Import_and_Export.check_enough_teu(100, 40) == True

    # Недостаточно
    assert Import_and_Export.check_enough_teu(10, 40) == False


def test_get_sum_delta_teu():
    # Тест суммирования TEU
    data_result = [
        [{'count_container': 5}, {'count_container': 10}],
        [{'count_container': 3}, {'count_container': 7}]
    ]

    # (5*2 + 10) + (3*2 + 7) = 10 + 10 + 6 + 7 = 33
    assert Import_and_Export.get_sum_delta_teu(data_result) == 33


def test_sum_delta_count():
    # Тест подсчета суммы delta_teo
    data = [
        [{'container_size': 40, 'count_container': 5}, {'container_size': 20, 'count_container': 10}],
        [{'container_size': 40, 'count_container': 3}, {'container_size': 20, 'count_container': 7}]
    ]

    # (5*2) + 10 + (3*2) + 7 = 10 + 10 + 6 + 7 = 33
    assert Import_and_Export.sum_delta_count(data) == 33


def test_get_terminal():
    # Тест преобразования названия терминала
    assert Import_and_Export.get_terminal("NMTP") == "НМТП"
    assert Import_and_Export.get_terminal("NLE") == "НЛЭ"
    assert Import_and_Export.get_terminal("OTHER") is None


# Тесты для методов экземпляра класса Import_and_Export
def test_get_data(import_export):
    # Тест получения данных из DataFrame
    data_df = pd.Series({
        'operator': 'TestLine',
        'ship_name_unified': 'TestShip',
        'stividor': 'NMTP',
        'atb_moor_pier': '2023-01-01',
        'vessel': 'TestVessel'
    })

    result = import_export.get_data(data_df)

    assert result == {
        'line': 'TestLine',
        'ship': 'TestShip',
        'terminal': 'НМТП',
        'date': '2023-01-01',
        'is_empty': False,
        'vessel': 'TestVessel'
    }


def test_get_index_df(import_export):
    # Тест получения индекса из DataFrame
    data_dis = pd.DataFrame({
        'atb_moor_pier': ['2023-01-01', '2023-01-02'],
        'operator': ['Line1', 'Line2'],
        'ship_name_unified': ['Ship1', 'Ship2']
    })

    # Существующий индекс
    index = import_export.get_index_df(data_dis, '2023-01-01', 'Line1', 'Ship1')
    assert index == 0

    # Несуществующий индекс
    index = import_export.get_index_df(data_dis, '2023-01-03', 'Line3', 'Ship3')
    assert index is False


def test_get_different_df(import_export):
    # Тест получения разницы в DataFrame
    data_dis = pd.DataFrame({
        'atb_moor_pier': ['2023-01-01', '2023-01-02'],
        'operator': ['Line1', 'Line2'],
        'ship_name_unified': ['Ship1', 'Ship2'],
        'delta_count': [100, 200]
    })

    data_result_dis = [
        [
            {'line': 'Line1', 'ship': 'Ship1', 'date': '2023-01-01', 'count_container': 30},
            {'line': 'Line1', 'ship': 'Ship1', 'date': '2023-01-01', 'count_container': 20}
        ]
    ]

    result_df = import_export.get_different_df(data_dis, data_result_dis)

    # Проверяем, что значение delta_count уменьшилось на 50
    assert result_df.loc[0, 'delta_count'] == 50


def test_change_20_test(import_export):
    # Тест изменения 20-футового контейнера на 40-футовый
    data_result = [
        [
            {'count_container': 50},
            {'count_container': 20}
        ],
        [
            {'count_container': 30},
            {'count_container': 10}
        ]
    ]

    # Изменяем 20 на индексе с наибольшим значением count_container
    result = import_export.change_20_test(data_result, 10)

    # Проверяем, что count_container изменились правильно
    assert result[0][0]['count_container'] == 40  # 50 - 10
    assert result[0][1]['count_container'] == 30  # 20 + 10


def test_change_40_test(import_export):
    # Тест изменения 40-футового контейнера на 20-футовый
    data_result = [
        [
            {'count_container': 50},
            {'count_container': 20}
        ],
        [
            {'count_container': 30},
            {'count_container': 10}
        ]
    ]

    # Изменяем 40 на индексе с наибольшим значением count_container
    result = import_export.change_40_test(data_result, 5)

    # Проверяем, что count_container изменились правильно
    assert result[0][0]['count_container'] == 55  # 50 + 5
    assert result[0][1]['count_container'] == 15  # 20 - 5


def test_check_delta_teu(import_export):
    # Тест проверки delta_teu с заполненным результатом
    data_result = [
        [
            {'container_size': 20, 'count_container': 10},
            {'container_size': 40, 'count_container': 5}
        ]
    ]

    # Сумма TEU: (10*1) + (5*2) = 10 + 10 = 20
    delta_teu = 20

    result = import_export.check_delta_teu(data_result, delta_teu)

    # Проверяем, что результат не изменился, так как сумма уже равна delta_teu
    assert result == data_result


def test_add_columns(import_export):
    # Тест добавления колонок
    data_df = pd.Series({
        'delta_count': 100,
        'operator': 'TestLine',
        'ship_name_unified': 'TestShip',
        'stividor': 'NMTP',
        'atb_moor_pier': '2023-01-01',
        'vessel': 'TestVessel'
    })

    percent40 = 40  # 40% контейнеров 40-футовые

    result = import_export.add_columns(data_df, percent40)

    # Проверяем распределение контейнеров
    assert result[0]['count_container'] == 40  # 40% от 100
    assert result[1]['count_container'] == 60  # 60% от 100

    # Проверяем размеры контейнеров
    assert result[0]['container_size'] == 40
    assert result[1]['container_size'] == 20


def test_filling_in_data(import_export):
    # Тест заполнения данных
    df = pd.DataFrame({
        'delta_count': [100, 50],
        'operator': ['Line1', 'Line2'],
        'ship_name_unified': ['Ship1', 'Ship2'],
        'stividor': ['NMTP', 'NLE'],
        'atb_moor_pier': ['2023-01-01', '2023-01-02'],
        'vessel': ['Vessel1', 'Vessel2']
    })

    percent = 40

    result = import_export.filling_in_data(percent, df)

    # Проверяем количество групп контейнеров
    assert len(result) == 2

    # Проверяем данные для первой группы
    assert result[0][0]['line'] == 'Line1'
    assert result[0][0]['count_container'] == 40  # 40% от 100
    assert result[0][1]['count_container'] == 60  # 60% от 100

    # Проверяем данные для второй группы
    assert result[1][0]['line'] == 'Line2'
    assert result[1][0]['count_container'] == 20  # 40% от 50
    assert result[1][1]['count_container'] == 30  # 60% от 50


# Тесты для класса Ref
def test_change_df(ref_class):
    # Тест изменения DataFrame
    df = pd.DataFrame({
        'delta_count': [100, 50, 30]
    })

    diff = 60

    result = ref_class.change_df(df, diff)

    # Проверяем, что сумма delta_count уменьшилась на diff
    assert result['delta_count'].sum() == 120  # 180 - 60


def test_get_index_to_df(ref_class):
    # Тест получения индекса из DataFrame
    data_dis = pd.DataFrame({
        'atb_moor_pier': ['2023-01-01', '2023-01-02'],
        'operator': ['Line1', 'Line2'],
        'ship_name_unified': ['Ship1', 'Ship2']
    })

    data = [
        {'line': 'Line1', 'ship': 'Ship1', 'date': '2023-01-01', 'count_container': 30},
        {'line': 'Line1', 'ship': 'Ship1', 'date': '2023-01-01', 'count_container': 20}
    ]

    index, count = ref_class.get_index_to_df(data_dis, data)

    assert index == 0
    assert count == 50  # 30 + 20


# Тесты для класса Empty
def test_preliminary_processing(empty_class):
    # Тест предварительной обработки
    df = pd.DataFrame({
        'delta_count': [100, 50],
        'operator': ['Line1', 'Line2'],
        'ship_name_unified': ['Ship1', 'Ship2'],
        'stividor': ['NMTP', 'NLE'],
        'atb_moor_pier': ['2023-01-01', '2023-01-02'],
        'vessel': ['Vessel1', 'Vessel2']
    })

    # Мокаем методы и атрибуты
    empty_class.clickhouse.get_delta_teu.return_value = 200
    empty_class.data_no_is_empty_ref = Mock(return_value=[
        [{'count_container': 40}, {'count_container': 60}]
    ])
    empty_class.sum_delta_count = Mock(return_value=140)

    empty_class.preliminary_processing(df)

    # Проверяем, что результаты установлены правильно
    assert empty_class.result_empty == [
        [{'count_container': 40}, {'count_container': 60}]
    ]
    assert empty_class.delta_teu == 60  # 200 - 140


def test_start_with_data(empty_class):
    # Тест метода start с данными
    df = pd.DataFrame({
        'delta_count': [100, 50],
        'operator': ['Line1', 'Line2'],
        'ship_name_unified': ['Ship1', 'Ship2'],
        'stividor': ['NMTP', 'NLE'],
        'atb_moor_pier': ['2023-01-01', '2023-01-02'],
        'vessel': ['Vessel1', 'Vessel2']
    })

    # Устанавливаем delta_teu
    empty_class.delta_teu = 100
    empty_class.result_empty = [
        [{'count_container': 20}, {'count_container': 30}]
    ]

    # Мокаем метод
    empty_class.data_no_is_empty = Mock(return_value=[
        [{'count_container': 30}, {'count_container': 40}]
    ])

    result = empty_class.start(df)

    # Проверяем результат
    assert result == [
        [{'count_container': 30}, {'count_container': 40}],
        [{'count_container': 20}, {'count_container': 30}]
    ]


def test_start_with_no_data(empty_class):
    # Тест метода start без данных
    # Устанавливаем delta_teu в отрицательное значение
    empty_class.delta_teu = -10
    empty_class.result_empty = [
        [{'count_container': 20}, {'count_container': 30}]
    ]

    result = empty_class.start(None)

    # Проверяем результат, должен вернуть result_empty
    assert result == [
        [{'count_container': 20}, {'count_container': 30}]
    ]


# Тесты для класса Extrapolate
def test_sample_difference_from(extrapolate_class):
    # Тест метода sample_difference_from
    df = pd.DataFrame({
        'delta_count': [100, 50, 30]
    })

    sum_container = 80

    result = extrapolate_class.sample_difference_from(sum_container, df)

    # Проверяем, что сумма delta_count уменьшилась на sum_container
    assert result['delta_count'].sum() == 100  # 180 - 80

    # Проверяем, что наибольшие значения были уменьшены первыми
    assert result.loc[0, 'delta_count'] < 100


def test_check_enough_container(extrapolate_class):
    # Тест метода check_enough_container
    # Мокаем методы и атрибуты
    extrapolate_class.empty.clickhouse.get_delta_teu.return_value = 100
    extrapolate_class.import_end_export.clickhouse.get_delta_teu.return_value = 200
    extrapolate_class.import_end_export.clickhouse.get_table_in_db_positive.side_effect = [
        (150, pd.DataFrame({'delta_count': [100, 50], 'operator': ['Line1', 'Line2']})),
        (50, pd.DataFrame({'delta_count': [30, 20], 'operator': ['Line3', 'Line4']}))
    ]
    extrapolate_class.ref.clickhouse.terminal = 'not_nmtp'

    result, flag = extrapolate_class.check_enough_container()

    # Проверяем результат
    assert flag == 'dis'  # флаг для всех контейнеров
    assert isinstance(result, pd.DataFrame)


def test_distribution_of_containers_by_ports(extrapolate_class):
    # Тест метода distribution_of_containers_by_ports
    data = {'count_container': 100}
    df = pd.DataFrame({
        'tracking_seaport': ['Port1', 'Port2', 'Port3'],
        'percent': [50, 30, 20]
    })

    result = extrapolate_class.distribution_of_containers_by_ports(data, df)

    # Проверяем распределение по портам
    assert result == {'Port1': 50, 'Port2': 30, 'Port3': 20}


def test_filling_count_to_percent(extrapolate_class):
    # Тест метода filling_count_to_percent
    data = {'count_container': 100}
    df = pd.DataFrame({
        'tracking_seaport': ['Port1', 'Port2', 'Port3'],
        'percent': [50, 30, 20]
    })

    result = extrapolate_class.filling_count_to_percent(data, df)

    # Проверяем проценты
    assert result == {'Port1': 50, 'Port2': 30, 'Port3': 20}


def test_filling_in_missing_data(extrapolate_class):
    # Тест метода filling_in_missing_data
    data_port = {'Port1': 45, 'Port2': 25, 'Port3': 20}
    data = {'count_container': 100}

    result = extrapolate_class.filling_in_missing_data(data_port, data)

    # Проверяем, что общая сумма равна count_container
    assert sum(result.values()) == 100
    # Проверяем, что наибольший порт получил недостающее количество
    assert result['Port1'] == 55  # 45 + 10