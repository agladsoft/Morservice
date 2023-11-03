import sys
import os
import httpx
import app_logger
from datetime import datetime
import pandas as pd
from pandas import DataFrame,Series
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import QueryResult
from typing import *


PARAMETRS = ['LIDER LINE','INERCONT (GAP RESOURSE)','UCAK LINE']