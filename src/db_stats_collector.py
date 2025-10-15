# db_stats_collector.py
import urllib.parse
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from loguru import logger


@dataclass
class TableStatistics:
    table_name: str
    row_count: int
    size_bytes: int
    column_stats: Dict[str, Dict]
    index_usage: List[Dict]
    last_accessed: Optional[str]
    partitioning_columns: Optional[List[str]] = None


@dataclass
class ConnectionInfo:
    driver: str
    host: str
    port: int
    database: str  # This is actually catalog in Trino
    username: str
    password: str
    additional_params: Dict[str, str]


def convert_numpy_types(obj):
    """Конвертирует numpy/pandas типы в обычные Python типы для JSON сериализации"""
    if isinstance(obj, (np.integer, pd.Int64Dtype)):
        return int(obj)
    elif isinstance(obj, (np.floating, pd.Float64Dtype)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (pd.Timestamp, pd.DatetimeTZDtype)):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    return obj


def safe_json_serialize(data: Any) -> Dict:
    """Безопасно сериализует данные, конвертируя numpy/pandas типы"""
    if isinstance(data, dict):
        return {k: safe_json_serialize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [safe_json_serialize(item) for item in data]
    else:
        return convert_numpy_types(data)


class DatabaseStatsCollector:
    def __init__(self, connection_url: str):
        self.connection_info = self._parse_connection_url(connection_url)
        self.engine = None
        self.sqlalchemy_url = self._build_sqlalchemy_url()
        self.actual_catalog = None  # Будет определен при подключении

    def _parse_connection_url(self, url: str) -> ConnectionInfo:
        """Парсит JDBC URL"""
        if not url.startswith('jdbc:'):
            raise ValueError("Invalid JDBC URL format")

        # Убираем jdbc: префикс
        url = url[5:]

        # Определяем драйвер
        driver_match = url.split('://')[0]
        driver = driver_match

        # Парсим остальную часть URL
        parsed = urllib.parse.urlparse(url)

        # Извлекаем параметры
        params = urllib.parse.parse_qs(parsed.query)

        # Для Trino: если база не указана, используем пустую строку (не 'default'!)
        database = parsed.path.lstrip('/') if parsed.path and parsed.path != '/' else ''

        return ConnectionInfo(
            driver=driver,
            host=parsed.hostname,
            port=parsed.port or (443 if driver == 'trino' else 5432),
            database=database,
            username=params.get('user', [''])[0],
            password=params.get('password', [''])[0],
            additional_params={k: v[0] for k, v in params.items()
                               if k not in ['user', 'password']}
        )

    def _build_sqlalchemy_url(self) -> str:
        """Строит URL для SQLAlchemy"""
        encoded_password = urllib.parse.quote(self.connection_info.password, safe='')

        # Для Trino не добавляем database в URL, если он пустой
        if self.connection_info.database:
            sqlalchemy_url = (
                f"{self.connection_info.driver}://"
                f"{self.connection_info.username}:{encoded_password}@"
                f"{self.connection_info.host}:{self.connection_info.port}/"
                f"{self.connection_info.database}"
            )
        else:
            sqlalchemy_url = (
                f"{self.connection_info.driver}://"
                f"{self.connection_info.username}:{encoded_password}@"
                f"{self.connection_info.host}:{self.connection_info.port}"
            )

        return sqlalchemy_url

    def _detect_catalog(self, conn) -> Optional[str]:
        """Определяет доступный каталог в Trino"""
        try:
            # Получаем список всех каталогов
            catalogs_df = pd.read_sql("SHOW CATALOGS", conn)

            if catalogs_df.empty:
                logger.warning("No catalogs found in Trino")
                return None

            catalogs = catalogs_df.iloc[:, 0].tolist()
            logger.info(f"Available catalogs: {catalogs}")

            # Приоритет выбора каталога:
            # 1. Если указан в connection_info.database
            if self.connection_info.database and self.connection_info.database in catalogs:
                return self.connection_info.database

            # 2. Ищем системные каталоги (обычно есть всегда)
            for preferred in ['hive', 'iceberg', 'memory', 'tpch']:
                if preferred in catalogs:
                    return preferred

            # 3. Берем первый доступный (кроме system)
            for catalog in catalogs:
                if catalog.lower() != 'system':
                    return catalog

            # 4. В крайнем случае - system
            return 'system' if 'system' in catalogs else catalogs[0]

        except Exception as e:
            logger.error(f"Error detecting catalog: {e}")
            return None

    def connect(self) -> bool:
        """Устанавливает соединение с БД через SQLAlchemy"""
        try:
            self.engine = create_engine(self.sqlalchemy_url)

            # Тестовый запрос для проверки соединения
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()

                # Определяем каталог для Trino
                if self.connection_info.driver == 'trino':
                    self.actual_catalog = self._detect_catalog(conn)
                    if self.actual_catalog:
                        logger.info(f"Using catalog: {self.actual_catalog}")
                    else:
                        logger.warning("Could not determine catalog")

            logger.info(f"Successfully connected to {self.connection_info.driver} database")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def collect_table_statistics(self, tables: List[str]) -> List[TableStatistics]:
        """Собирает статистику по таблицам"""
        if not self.engine:
            logger.error("No database connection")
            return []

        stats = []

        for table in tables:
            try:
                table_stats = self._get_single_table_stats(table)
                if table_stats:
                    stats.append(table_stats)
            except Exception as e:
                logger.error(f"Failed to collect stats for table {table}: {e}")

        return stats

    def _get_single_table_stats(self, table_name: str) -> Optional[TableStatistics]:
        """Собирает статистику по одной таблице (обновленная версия)"""
        try:
            with self.engine.connect() as conn:
                # Подсчет строк
                count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
                row_count = int(conn.execute(text(count_query)).scalar_one())

                # Информация о колонках
                column_stats = self._get_column_statistics(conn, table_name)

                # Получаем реальные метаданные таблицы
                table_metadata = self._get_table_metadata(conn, table_name)

                return TableStatistics(
                    table_name=table_name,
                    row_count=row_count,
                    size_bytes=table_metadata.get('size_bytes', 0),
                    column_stats=column_stats,
                    partitioning_columns=table_metadata.get('partitioning', []),
                    index_usage=[],
                    last_accessed=None
                )

        except Exception as e:
            logger.error(f"Error getting stats for {table_name}: {e}")
            return None

    def _get_table_metadata(self, conn, table_name: str) -> Dict:
        """Получает метаданные таблицы, включая размер и партиционирование, через SHOW STATS."""
        metadata = {}
        try:
            # Этот запрос предоставляет подробную статистику в Trino, включая размер данных
            stats_query = f"SHOW STATS FOR {table_name}"
            stats_df = pd.read_sql(stats_query, conn)

            total_stats = stats_df[stats_df['column_name'].isnull()]
            if not total_stats.empty:
                if 'data_size' in total_stats.columns and not pd.isna(total_stats['data_size'].iloc[0]):
                    metadata['size_bytes'] = int(total_stats['data_size'].iloc[0])

            # (синтаксис может зависеть от коннектора: Hive, Iceberg и т.д.)
            # desc_query = f"DESCRIBE {table_name}"
            # ... парсинг вывода DESCRIBE для поиска partitioning keys ...

        except Exception as e:
            logger.warning(f"Could not get detailed metadata for {table_name} via SHOW STATS: {e}")
            if 'size_bytes' not in metadata:
                metadata['size_bytes'] = self._estimate_table_size(conn, table_name, 0)  # row_count здесь не нужен

        return metadata

    def _get_column_statistics(self, conn, table_name: str) -> Dict[str, Dict]:
        """Собирает статистику по колонкам"""
        column_stats = {}

        try:
            # Парсим имя таблицы
            parts = table_name.split('.')
            if len(parts) == 3:
                catalog, schema, table = parts
            elif len(parts) == 2:
                catalog = self.actual_catalog or self.connection_info.database
                schema, table = parts
            else:
                catalog = self.actual_catalog or self.connection_info.database
                schema = 'default'  # Trino обычно использует 'default' schema
                table = parts[0]

            if not catalog:
                logger.error(f"Cannot determine catalog for table {table_name}")
                return {}

            # Запрос информации о колонках
            columns_query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_catalog = '{catalog}' 
                AND table_schema = '{schema}' 
                AND table_name = '{table}'
                ORDER BY ordinal_position
            """

            logger.debug(f"Executing columns query: {columns_query}")
            columns_df = pd.read_sql(columns_query, conn)

            # Ограничиваем количество колонок для анализа
            for idx, row in columns_df.iterrows():
                column_name = row['column_name']
                data_type = row['data_type']

                try:
                    # Базовая статистика
                    basic_stats_query = f"""
                        SELECT 
                            COUNT(DISTINCT {column_name}) as distinct_count,
                            COUNT(*) - COUNT({column_name}) as null_count
                        FROM {table_name}
                    """

                    stats_result = conn.execute(text(basic_stats_query))
                    stats_row = stats_result.fetchone()

                    column_stats[column_name] = {
                        'data_type': data_type,
                        'distinct_count': int(stats_row[0]) if stats_row[0] is not None else 0,
                        'null_count': int(stats_row[1]) if stats_row[1] is not None else 0
                    }

                    # Числовая статистика
                    if data_type.lower() in ['integer', 'bigint', 'double', 'real', 'decimal']:
                        try:
                            numeric_stats_query = f"""
                                SELECT 
                                    MIN({column_name}) as min_value,
                                    MAX({column_name}) as max_value,
                                    AVG(CAST({column_name} AS DOUBLE)) as avg_value
                                FROM {table_name}
                                WHERE {column_name} IS NOT NULL
                            """

                            numeric_result = conn.execute(text(numeric_stats_query))
                            numeric_row = numeric_result.fetchone()

                            if numeric_row and any(x is not None for x in numeric_row):
                                column_stats[column_name].update({
                                    'min_value': float(numeric_row[0]) if numeric_row[0] is not None else None,
                                    'max_value': float(numeric_row[1]) if numeric_row[1] is not None else None,
                                    'avg_value': float(numeric_row[2]) if numeric_row[2] is not None else None
                                })
                        except Exception as num_e:
                            logger.warning(f"Failed to get numeric stats for {column_name}: {num_e}")

                except Exception as e:
                    logger.warning(f"Failed to get stats for column {column_name}: {e}")
                    column_stats[column_name] = {
                        'data_type': data_type,
                        'error': str(e)
                    }

        except Exception as e:
            logger.error(f"Failed to get column stats for {table_name}: {e}")

        return column_stats

    def _estimate_table_size(self, conn, table_name: str, row_count: int) -> int:
        """Оценивает размер таблицы"""
        try:
            if row_count > 0:
                estimated_row_size = 200  # байт на строку
                return row_count * estimated_row_size
            return 0

        except Exception as e:
            logger.warning(f"Could not estimate table size for {table_name}: {e}")
            return 0

    def get_database_overview(self) -> Dict:
        """Получает общую информацию о БД"""
        if not self.engine:
            return {'error': 'No database connection'}

        overview = {
            'driver': self.connection_info.driver,
            'host': self.connection_info.host,
            'database': self.connection_info.database,
            'actual_catalog': self.actual_catalog,
            'connection_successful': True
        }

        try:
            with self.engine.connect() as conn:
                # Версия БД
                if self.connection_info.driver == 'trino':
                    version_df = pd.read_sql("SELECT version()", conn)
                    overview['version'] = str(version_df.iloc[0, 0])

                # Используем фактический каталог
                catalog = self.actual_catalog
                if not catalog:
                    overview['error'] = 'No catalog available'
                    return safe_json_serialize(overview)

                # Список схем
                schemas_query = f"""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE catalog_name = '{catalog}'
                """
                logger.debug(f"Executing schemas query: {schemas_query}")
                schemas_df = pd.read_sql(schemas_query, conn)
                overview['schemas'] = [str(schema) for schema in schemas_df['schema_name'].tolist()]

                # Общее количество таблиц
                tables_query = f"""
                    SELECT COUNT(*) as table_count
                    FROM information_schema.tables 
                    WHERE table_catalog = '{catalog}'
                """
                tables_df = pd.read_sql(tables_query, conn)
                overview['total_tables'] = int(tables_df.iloc[0, 0])

        except Exception as e:
            overview['error'] = str(e)
            logger.error(f"Error getting database overview: {e}")

        return safe_json_serialize(overview)

    def close(self):
        """Закрывает соединение с БД"""
        if self.engine:
            self.engine.dispose()
            self.engine = None


def test_connection(jdbc_url: str) -> bool:
    """Тестирует подключение к базе данных"""
    try:
        collector = DatabaseStatsCollector(jdbc_url)
        success = collector.connect()

        if success:
            overview = collector.get_database_overview()
            print("Connection successful!")
            print(f"Database overview: {json.dumps(overview, indent=2, ensure_ascii=False)}")

            # Тестовый запрос
            try:
                catalog = collector.actual_catalog
                if catalog:
                    with collector.engine.connect() as conn:
                        test_df = pd.read_sql(f"""
                            SELECT table_schema, table_name 
                            FROM information_schema.tables 
                            WHERE table_catalog = '{catalog}'
                            LIMIT 5
                        """, conn)
                        print(f"\nFound {len(test_df)} tables:")
                        if not test_df.empty:
                            for _, row in test_df.iterrows():
                                print(f"  - {row['table_schema']}.{row['table_name']}")
            except Exception as query_e:
                print(f"Test query failed: {query_e}")

        collector.close()
        return success

    except Exception as e:
        print(f"Connection test failed: {e}")
        return False


if __name__ == "__main__":
    test_url = "jdbc:trino://trino.czxqx2r9.data.bizmrg.com:443?user=hackuser&password=dovq(ozaq8ngt)oS"
    success = test_connection(test_url)
    print(f"\nConnection test result: {'SUCCESS' if success else 'FAILED'}")