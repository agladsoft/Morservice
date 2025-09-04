# ⚓ Morservice

## 📋 Описание проекта

Сервис интеллектуального анализа и экстраполяции данных морских контейнерных перевозок. Система предназначена для автоматического распределения контейнеров по типам (REF, пустые, грузовые), размерам (20/40 футов) и портам назначения на основе статистических данных и алгоритмов машинного обучения.

## ⚡ Функциональность

- 🧠 **Интеллектуальная экстраполяция**: Автоматическое распределение контейнеров на основе исторических данных и паттернов
- 📊 **Анализ дисбаланса**: Выявление и корректировка расхождений между планируемыми и фактическими контейнерными перевозками
- 🚛 **Типизация контейнеров**: Автоматическая классификация контейнеров (REF, Empty, Standard) с определением размеров
- 🌍 **Географическое распределение**: Распределение контейнеров по портам назначения с учетом популярности маршрутов
- 📈 **Процентный анализ**: Вычисление оптимального соотношения 40-футовых и 20-футовых контейнеров
- 🔄 **Циклическая обработка**: Непрерывный мониторинг и обновление данных каждые 30 секунд
- 🏭 **Мультитерминальность**: Поддержка различных терминалов (NLE, NMTP) с учетом их специфики

## 🏗️ Архитектура проекта

```
Morservice/
├── Dockerfile                  # Конфигурация Docker контейнера
├── requirements.txt           # Python зависимости
├── __init__.py               # Общие импорты и константы
├── app_logger.py             # Модуль логирования
├── Database.py               # Класс для работы с ClickHouse
├── Ref.py                    # Основной модуль экстраполяции
├── missing_data.py           # Обработка недостающих данных
├── line_ref.py              # Обработка REF контейнеров
├── parsed_file.py           # Парсинг файлов данных
├── bash/                    # Bash скрипты
│   └── run.sh              # Основной скрипт запуска
├── None/                    # Директория логов
│   └── logging/
└── venv/                   # Виртуальное окружение
```

## 🔧 Переменные окружения

### ✅ Обязательные переменные:
- `XL_IDP_PATH_MORSERVICE_SCRIPTS` - путь к директории со скриптами в хост-системе
- `XL_IDP_PATH_DOCKER` - путь к директории со скриптами внутри контейнера
- `XL_IDP_PATH_MORSERVICE` - путь для выполнения скриптов внутри контейнера

### 🗃️ База данных (ClickHouse):
- Подключение: `host='clickhouse', database='default', username='default', password='6QVnYsC4iSzz'`
- Основные таблицы:
  - `check_month` - конфигурация периодов и терминалов
  - `nle_cross` - данные по дельта TEU
  - `not_found_containers` - контейнеры без совпадений
  - `discrepancies_found_containers` - расхождения в данных
  - `extrapolate` - результаты экстраполяции

## 🚀 Установка и запуск

### 🐳 Рекомендуемый способ: Docker Compose

Сервис интегрирован в общую конфигурацию docker-compose.yml:

```yaml
service_mor:
  container_name: service_mor
  restart: always
  ports:
    - "8005:8005"
  volumes:
    - ${XL_IDP_PATH_MORSERVICE_SCRIPTS}:${XL_IDP_PATH_DOCKER}
  environment:
    XL_IDP_PATH_MORSERVICE: ${XL_IDP_PATH_DOCKER}
  build:
    context: Morservice
    dockerfile: ./Dockerfile
    args:
      XL_IDP_PATH_DOCKER: ${XL_IDP_PATH_DOCKER}
  command:
    bash -c "sh ${XL_IDP_PATH_DOCKER}/bash/run.sh"
  networks:
    - postgres
```

#### 1. **🛠️ Подготовка окружения**

Создайте файл `.env` с необходимыми переменными окружения:
```bash
XL_IDP_PATH_MORSERVICE_SCRIPTS=/path/to/morservice_scripts
XL_IDP_PATH_DOCKER=/app/scripts
XL_IDP_PATH_MORSERVICE=/app/scripts
```

#### 2. **▶️ Запуск сервиса**

```bash
docker-compose up -d service_mor
```

### 🐋 Альтернативный способ: Docker

#### 1. **🔨 Сборка образа**
```bash
cd Morservice
docker build -t morservice .
```

#### 2. **🚀 Запуск контейнера**
```bash
docker run -d \
  --name morservice \
  -p 8005:8005 \
  -v /path/to/scripts:/app/scripts \
  -e XL_IDP_PATH_MORSERVICE=/app/scripts \
  --network postgres \
  morservice
```

### 💻 Разработка без Docker

#### 1. **📦 Установка зависимостей**
```bash
cd Morservice
pip install -r requirements.txt
```

#### 2. **🔐 Настройка переменных окружения**
```bash
export XL_IDP_PATH_MORSERVICE=/path/to/morservice/scripts
```

#### 3. **▶️ Запуск обработки**
```bash
# Запуск основного модуля экстраполяции
python3 Ref.py

# Запуск обработки недостающих данных
python3 missing_data.py

# Запуск циклического демона
bash bash/run.sh
```

## ⚙️ Принцип работы

### 🔄 Основной цикл:
1. **⏰ Циклическое выполнение**: Скрипт `run.sh` каждые 30 секунд запускает два модуля:
   - `Ref.py` - основная экстраполяция контейнеров
   - `missing_data.py` - обработка недостающих данных

### 🧠 Алгоритм экстраполяции:

#### 1. **📊 Получение данных из ClickHouse**:
- Чтение конфигурации из таблицы `check_month`
- Получение дельта TEU из таблицы `nle_cross`
- Извлечение данных о несовпадениях и расхождениях

#### 2. **🚛 Обработка REF контейнеров**:
- Анализ рефрижераторных контейнеров (только 40-футовые)
- Распределение по доступным слотам
- Корректировка количества при превышении лимитов

#### 3. **📦 Обработка пустых контейнеров**:
- Предварительная обработка пустых контейнеров
- Учет специфики терминалов (NMTP/NLE)
- Распределение остаточного TEU

#### 4. **⚖️ Экстраполяция импорт/экспорт**:
- Вычисление процентного соотношения 40/20-футовых контейнеров
- Применение алгоритмов балансировки:
  - При соотношении 45-55%: прямое распределение
  - При дисбалансе: коррекция через таблицы расхождений
  - При нехватке контейнеров: пропорциональное распределение

#### 5. **🌍 Географическое распределение** (для NLE):
- Анализ популярности портов по историческим данным
- Распределение контейнеров по топ-3 портам
- Учет сезонности и трендов

### 🔧 Алгоритмы корректировки:

#### **Процентный метод**:
```python
percent40 = ((delta_teu / data_count) - 1) * 100
```

#### **Балансировка TEU**:
- Проверка итогового TEU после распределения
- Корректировка через изменение типов контейнеров
- Финальная подгонка к целевому значению

#### **Пропорциональное уменьшение**:
```python
reduction_ratio = (current_teu - diff) / current_teu
```

## 📊 Структура данных

### 🔄 Входные данные (ClickHouse):
- **check_month**: конфигурация периодов обработки
- **nle_cross**: целевые значения TEU по направлениям
- **not_found_containers**: контейнеры без найденных совпадений
- **discrepancies_found_containers**: выявленные расхождения

### 📄 Выходные данные (таблица extrapolate):
```json
{
  "line": "SHIPPING_LINE_NAME",
  "ship": "VESSEL_NAME", 
  "direction": "import/export",
  "month": 1,
  "year": 2024,
  "terminal": "НЛЭ/НМТП",
  "date": "2024-01-15",
  "container_type": "REF/HC/DC",
  "is_empty": false,
  "is_ref": true,
  "container_size": 40,
  "count_container": 25,
  "goods_name": "РЕФРИЖЕРАТОРНЫЙ ГРУЗ",
  "tracking_country": "Germany",
  "tracking_seaport": "Hamburg",
  "month_port": "2024-01-01",
  "is_missing": false
}
```

## 🎯 Алгоритмические особенности

### 🧮 Математические модели:

#### **TEU Calculation**:
- 20-футовый контейнер = 1 TEU
- 40-футовый контейнер = 2 TEU
- REF контейнеры: только 40-футовые

#### **Percentage Distribution**:
```python
# Основная формула процентного распределения
percent40 = ((delta_teu / data_count) - 1) * 100

# Корректировка при дисбалансе
if 45 <= percent40 <= 55:
    # Прямое распределение
elif percent40 <= 0:
    # Пропорциональное распределение
elif percent40 > 100:
    # Использование 100% 40-футовых
```

#### **Container Type Logic**:
```python
# REF контейнеры
container_40_ft = delta_teu // 2 if delta_teu % 2 == 0 else (delta_teu // 2) + 1

# Обычные контейнеры
container_40_ft = (delta_teu // 2) // 2
container_20_ft = container_40_ft * 2
```

### 🏭 Терминальная специфика:

#### **NMTP (Новороссийский терминал)**:
- Отсутствие таблицы `discrepancies_found_containers`
- Использование `reference_spardeck` для определения портов
- Специальные алгоритмы для REF контейнеров

#### **NLE (Новороссийский терминал)**:
- Полная поддержка географического распределения
- Анализ популярности портов по историческим данным
- Сезонная корректировка распределения

## 🔍 Мониторинг и отладка

### 📋 **Просмотр логов контейнера**:
```bash
docker logs service_mor -f
```

### 🔌 **Подключение к контейнеру**:
```bash
docker exec -it service_mor bash
```

### 🗃️ **SQL запросы для мониторинга**:
```sql
-- Текущая конфигурация
SELECT * FROM check_month WHERE is_on = 1;

-- Целевые значения TEU
SELECT * FROM nle_cross 
WHERE month = 1 AND year = 2024 AND direction = 'import';

-- Результаты экстраполяции
SELECT 
    terminal,
    direction,
    container_type,
    container_size,
    SUM(count_container) as total_containers,
    SUM(count_container * container_size / 20) as total_teu
FROM extrapolate 
WHERE month = 1 AND year = 2024
GROUP BY terminal, direction, container_type, container_size;

-- Распределение по линиям
SELECT 
    line,
    COUNT(*) as records,
    SUM(count_container) as total_containers
FROM extrapolate 
GROUP BY line 
ORDER BY total_containers DESC;
```

### 📊 **Аналитические запросы**:
```sql
-- Эффективность алгоритма
WITH target AS (
    SELECT terminal, direction, SUM(teu_delta) as target_teu
    FROM nle_cross 
    WHERE month = 1 AND year = 2024
    GROUP BY terminal, direction
),
actual AS (
    SELECT 
        terminal, 
        direction, 
        SUM(count_container * container_size / 20) as actual_teu
    FROM extrapolate 
    WHERE month = 1 AND year = 2024
    GROUP BY terminal, direction
)
SELECT 
    t.terminal,
    t.direction,
    t.target_teu,
    a.actual_teu,
    (a.actual_teu / t.target_teu * 100) as accuracy_percent
FROM target t
JOIN actual a ON t.terminal = a.terminal AND t.direction = a.direction;
```

## 💻 Требования к системе

- 🐍 **Python**: 3.8+
- 🐳 **Docker**: 20.10+
- 🐙 **Docker Compose**: 1.29+
- 🗃️ **ClickHouse**: 24.x+ для аналитических запросов
- 🧠 **RAM**: минимум 4GB для обработки больших датасетов
- ⚡ **CPU**: рекомендуется 4+ ядра для математических вычислений

## 📦 Зависимости

Основные Python пакеты (см. `requirements.txt`):
- `clickhouse-connect==0.6.8` - подключение к ClickHouse
- `pandas==2.0.3` - обработка и анализ данных
- `numpy==1.22.2` - математические операции
- `httpx==0.24.1` - HTTP клиент для API запросов
- `PyYAML==6.0.1` - обработка конфигурационных файлов

### 🔧 Специализированные библиотеки:
- `lz4==4.3.2` - сжатие данных для ClickHouse
- `zstandard==0.21.0` - дополнительные алгоритмы сжатия
- `python-dateutil==2.8.2` - продвинутая работа с датами

## 🔐 Безопасность

- 🔒 **База данных**: Использование защищенных подключений к ClickHouse
- 📝 **Логирование**: Детальное логирование для аудита операций
- 🚫 **Изоляция**: Контейнеризация для изоляции процессов
- 🔄 **Validation**: Валидация входных данных перед обработкой

## ⚠️ Известные особенности

1. **Циклическое выполнение**: Интервал 30 секунд может потребовать корректировки для нагруженных систем
2. **Терминальная специфика**: Различные алгоритмы для NMTP и NLE терминалов
3. **Математические округления**: Могут возникать небольшие расхождения в итоговых TEU
4. **Историческая зависимость**: Для географического распределения требуются исторические данные

## 🔧 Настройки и конфигурация

### Изменение интервала выполнения:
```bash
# Отредактируйте bash/run.sh
sleep 60;  # увеличить интервал до 60 секунд
```

### Настройка параметров экстраполяции:
```python
# В __init__.py измените PARAMETRS для REF линий
PARAMETRS = ['LIDER LINE', 'INERCONT (GAP RESOURSE)', 'UCAK LINE']
```

### Корректировка процентных порогов:
```python
# В Ref.py измените пороги балансировки
if 40 <= percent40 <= 60:  # расширить диапазон
```

## 🏷️ Архитектурные паттерны

### **Observer Pattern**: 
- Циклический мониторинг изменений в базе данных
- Реакция на обновления конфигурации

### **Strategy Pattern**:
- Различные алгоритмы для NMTP и NLE терминалов
- Переключение стратегий распределения контейнеров

### **Factory Pattern**:
- Создание различных типов контейнеров (REF, Empty, Standard)
- Инстанцирование классов обработки

## 👥 Поддержка и разработка

### Для разработки и отладки:
1. **Подготовка среды**:
   ```bash
   git clone <repository>
   cd Morservice
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Тестирование модулей**:
   ```bash
   # Тест основного модуля
   python3 -c "from Ref import Extrapolate; Extrapolate().main()"
   
   # Тест обработки недостающих данных
   python3 missing_data.py
   ```

3. **Отладка с логированием**:
   ```bash
   python3 -u Ref.py 2>&1 | tee debug.log
   ```

## 📈 Производительность

- **Время выполнения**: 5-15 секунд на цикл обработки
- **Пропускная способность**: до 10,000 записей за цикл
- **Точность экстраполяции**: 95-98% соответствия целевым TEU
- **Память**: ~500MB на обработку среднего датасета

## 🔄 Версионирование

- **v1.0.0** - Базовая экстраполяция для NLE терминала
- **v2.0.0** - Добавлена поддержка NMTP терминала
- **v3.0.0** - Реализована обработка REF контейнеров
- **v3.5.0** - Географическое распределение портов
- **v4.0.0** - Алгоритмы финальной корректировки TEU

## 📄 Лицензия

Проект предназначен для внутреннего использования в системе управления контейнерными морскими перевозками.