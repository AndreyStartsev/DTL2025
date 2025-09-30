# ddl_parser.py
import re
import json
from dataclasses import dataclass
from typing import List, Dict, Optional
import sqlparse


@dataclass
class Column:
    name: str
    data_type: str
    nullable: bool = True
    constraints: List[str] = None


@dataclass
class Table:
    catalog: str
    schema: str
    name: str
    columns: List[Column]
    indexes: List[str] = None
    constraints: List[str] = None


class DDLParser:
    def __init__(self):
        self.tables: List[Table] = []

    def parse_ddl_statements(self, ddl_list: List[Dict]) -> List[Table]:
        """Парсит DDL statements и извлекает структуру таблиц"""
        tables = []

        for ddl_item in ddl_list:
            statement = ddl_item['statement']
            table = self._parse_create_table(statement)
            if table:
                tables.append(table)

        return tables

    def _parse_create_table(self, ddl: str) -> Optional[Table]:
        """Парсит CREATE TABLE statement"""
        try:
            parsed = sqlparse.parse(ddl)[0]

            # Извлекаем имя таблицы
            table_name = self._extract_table_name(ddl)
            catalog, schema, name = self._parse_full_table_name(table_name)

            # Извлекаем колонки
            columns = self._extract_columns(ddl)

            return Table(
                catalog=catalog,
                schema=schema,
                name=name,
                columns=columns
            )
        except Exception as e:
            print(f"Error parsing DDL: {e}")
            return None

    def _extract_table_name(self, ddl: str) -> str:
        """Извлекает полное имя таблицы из DDL"""
        match = re.search(r'CREATE TABLE\s+([^\s(]+)', ddl, re.IGNORECASE)
        return match.group(1) if match else ""

    def _parse_full_table_name(self, full_name: str) -> tuple:
        """Разбирает полное имя таблицы на каталог, схему и имя"""
        parts = full_name.split('.')
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return "", parts[0], parts[1]
        else:
            return "", "", parts[0]

    def _extract_columns(self, ddl: str) -> List[Column]:
        """Извлекает информацию о колонках"""
        columns = []

        # Ищем содержимое между скобками после имени таблицы
        match = re.search(r'CREATE TABLE[^(]+\((.*)\)\s*WITH', ddl, re.IGNORECASE | re.DOTALL)
        if not match:
            return columns

        columns_text = match.group(1)

        # Разбиваем на отдельные определения колонок
        column_defs = self._split_column_definitions(columns_text)

        for col_def in column_defs:
            column = self._parse_column_definition(col_def.strip())
            if column:
                columns.append(column)

        return columns

    def _split_column_definitions(self, columns_text: str) -> List[str]:
        """Разбивает текст колонок на отдельные определения"""
        # Простое разбиение по запятым (может потребовать улучшения для сложных случаев)
        return [col.strip() for col in columns_text.split(',') if col.strip()]

    def _parse_column_definition(self, col_def: str) -> Optional[Column]:
        """Парсит определение одной колонки"""
        try:
            parts = col_def.split()
            if len(parts) < 2:
                return None

            name = parts[0]
            data_type = parts[1]

            # Обработка составных типов данных (например, varchar(255))
            if '(' in data_type and ')' not in data_type and len(parts) > 2:
                data_type += parts[2]

            return Column(name=name, data_type=data_type)
        except Exception as e:
            print(f"Error parsing column definition '{col_def}': {e}")
            return None

    def get_table_stats(self, tables: List[Table]) -> Dict:
        """Собирает статистику по таблицам"""
        return {
            'total_tables': len(tables),
            'total_columns': sum(len(table.columns) for table in tables),
            'tables_by_schema': self._group_by_schema(tables),
            'column_types_distribution': self._get_column_types_stats(tables)
        }

    def _group_by_schema(self, tables: List[Table]) -> Dict[str, int]:
        """Группирует таблицы по схемам"""
        schema_counts = {}
        for table in tables:
            schema_key = f"{table.catalog}.{table.schema}"
            schema_counts[schema_key] = schema_counts.get(schema_key, 0) + 1
        return schema_counts

    def _get_column_types_stats(self, tables: List[Table]) -> Dict[str, int]:
        """Статистика по типам колонок"""
        type_counts = {}
        for table in tables:
            for column in table.columns:
                base_type = column.data_type.split('(')[0].lower()
                type_counts[base_type] = type_counts.get(base_type, 0) + 1
        return type_counts

    def get_schema_insights(self, tables: List[Table], queries: List[Dict] = None) -> Dict:
        """
        Generate comprehensive schema insights for the analysis report.

        Args:
            tables: List of parsed Table objects
            queries: Optional list of query data for row estimation

        Returns:
            Dict with schema insights including tables, indexes, and coverage metrics
        """
        if not tables:
            return {
                "total_columns": 0,
                "total_tables": 0,
                "tables": [],
                "index_coverage": {
                    "indexed_tables": 0,
                    "total_indexes": 0,
                    "coverage_percent": 0,
                    "recommendations": "No tables found in schema."
                }
            }

        # Build table details
        table_details = []
        total_indexed = 0
        total_indexes = 0

        for table in tables:
            # Extract primary key info from constraints
            has_primary_key = self._has_primary_key(table)

            # Count indexes (from table.indexes if available)
            index_count = len(table.indexes) if table.indexes else 0
            if index_count > 0 or has_primary_key:
                total_indexed += 1
                total_indexes += index_count + (1 if has_primary_key else 0)

            # Estimate rows based on queries if available
            estimated_rows = self._estimate_table_rows(table, queries) if queries else 0

            table_details.append({
                "name": f"{table.schema}.{table.name}" if table.schema else table.name,
                "full_name": f"{table.catalog}.{table.schema}.{table.name}",
                "column_count": len(table.columns),
                "estimated_rows": estimated_rows,
                "has_primary_key": has_primary_key,
                "index_count": index_count
            })

        # Calculate coverage metrics
        total_tables = len(tables)
        coverage_percent = round((total_indexed / total_tables * 100), 1) if total_tables > 0 else 0

        # Generate recommendations
        recommendations = self._generate_index_recommendations(
            total_tables, total_indexed, total_indexes
        )

        return {
            "total_columns": sum(len(table.columns) for table in tables),
            "total_tables": total_tables,
            "tables": sorted(table_details, key=lambda x: x['column_count'], reverse=True),
            "index_coverage": {
                "indexed_tables": total_indexed,
                "total_indexes": total_indexes,
                "coverage_percent": coverage_percent,
                "recommendations": recommendations
            }
        }

    def _has_primary_key(self, table: Table) -> bool:
        """Check if table has a primary key constraint."""
        if not table.constraints:
            return False

        for constraint in table.constraints:
            if 'PRIMARY KEY' in constraint.upper():
                return True
        return False

    def _estimate_table_rows(self, table: Table, queries: List[Dict]) -> int:
        """
        Estimate table row count based on query execution data.
        This is a simple heuristic - you can improve it with actual row counts.
        """
        if not queries:
            return 0

        # Look for queries that reference this table
        table_name = table.name.lower()
        total_executions = 0

        for query_data in queries:
            query = query_data.get('query', '').lower()
            if f'from {table_name}' in query or f'join {table_name}' in query:
                # Use runquantity as a proxy for activity
                total_executions += query_data.get('runquantity', 0)

        # Simple heuristic: higher query volume suggests more rows
        # This is just an estimate - adjust the multiplier as needed
        if total_executions > 10000:
            return 1000000  # Large table
        elif total_executions > 1000:
            return 100000  # Medium table
        elif total_executions > 100:
            return 10000  # Small table
        else:
            return 1000  # Minimal table

    def _generate_index_recommendations(self, total_tables: int, indexed_tables: int, total_indexes: int) -> str:
        """Generate index coverage recommendations."""
        if total_tables == 0:
            return "No tables to analyze."

        coverage = (indexed_tables / total_tables) * 100

        if coverage < 30:
            return f"⚠️ Low index coverage ({coverage:.0f}%). Consider adding indexes to frequently queried tables."
        elif coverage < 70:
            return f"💡 Moderate index coverage ({coverage:.0f}%). Review query patterns to identify additional indexing opportunities."
        else:
            return f"✅ Good index coverage ({coverage:.0f}%). Monitor query performance to maintain optimal indexing."


# Пример использования
if __name__ == "__main__":
    # Пример входных данных
    input_data = {
        "ddl": [
            {
                "statement": "CREATE TABLE flights.public.flights ( flightdate date, airline varchar, origin varchar, dest varchar, cancelled boolean, diverted boolean, crsdeptime integer, deptime double, depdelayminutes double, depdelay double, arrtime double, arrdelayminutes double, airtime double, crselapsedtime double, actualelapsedtime double, distance double, year integer, quarter integer, month integer, dayofmonth integer, dayofweek integer, marketing_airline_network varchar, operated_or_branded_code_share_partners varchar, dot_id_marketing_airline integer, iata_code_marketing_airline varchar, flight_number_marketing_airline integer, operating_airline varchar, dot_id_operating_airline integer, iata_code_operating_airline varchar, tail_number varchar, flight_number_operating_airline integer, originairportid integer, originairportseqid integer, origincitymarketid integer, origincityname varchar, originstate varchar, originstatefips integer, originstatename varchar, originwac integer, destairportid integer, destairportseqid integer, destcitymarketid integer, destcityname varchar, deststate varchar, deststatefips integer, deststatename varchar, destwac integer, depdel15 double, departuredelaygroups double, deptimeblk varchar, taxiout double, wheelsoff double, wheelson double, taxiin double, crsarrtime integer, arrdelay double, arrdel15 double, arrivaldelaygroups double, arrtimeblk varchar, distancegroup integer, divairportlandings double ) WITH ( format = 'PARQUET', format_version = 2 );"
            }
        ]
    }

    parser = DDLParser()
    tables = parser.parse_ddl_statements(input_data['ddl'])
    stats = parser.get_table_stats(tables)

    print("Parsed tables:")
    for table in tables:
        print(f"  {table.catalog}.{table.schema}.{table.name} ({len(table.columns)} columns)")

    print(f"\nStatistics: {json.dumps(stats, indent=2)}")

    insights = parser.get_schema_insights(tables)
    print(f"\nSchema Insights: {json.dumps(insights, indent=2)}")