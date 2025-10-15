# query_analyzer.py
import re
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Function, Parenthesis
from typing import List, Dict, Set
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class QueryPattern:
    query_id: str
    query_type: str  # SELECT, INSERT, UPDATE, DELETE
    tables_used: Set[str]
    joins: List[Dict]
    aggregations: List[str]
    where_conditions: List[str]
    cte_usage: bool
    subqueries_count: int
    group_by_columns: List[str]
    order_by_columns: List[str]
    filter_columns: List[str]
    window_functions: List[str]
    run_quantity: int
    execution_time: float


class QueryAnalyzer:
    def __init__(self):
        self.patterns: List[QueryPattern] = []
        self.aggregate_functions = {
            'count', 'sum', 'avg', 'min', 'max', 'stddev', 'variance',
            'count_distinct', 'percentile', 'median'
        }
        self.window_functions = {
            'row_number', 'rank', 'dense_rank', 'lead', 'lag', 'first_value',
            'last_value', 'nth_value', 'ntile'
        }

    def analyze_queries(self, queries: List[Dict]) -> List[QueryPattern]:
        """Анализирует список SQL запросов"""
        patterns = []

        for query_data in queries:
            pattern = self._analyze_single_query(query_data)
            if pattern:
                patterns.append(pattern)

        return patterns

    def _analyze_single_query(self, query_data: Dict) -> QueryPattern:
        """Анализирует один SQL запрос"""
        query_id = query_data['queryid']
        query_text = query_data['query']
        run_quantity = query_data.get('runquantity', 0)
        execution_time = query_data.get('executiontime', 0)

        try:
            parsed = sqlparse.parse(query_text)[0]

            return QueryPattern(
                query_id=query_id,
                query_type=self._get_query_type(query_text),
                tables_used=self._extract_tables(query_text),
                joins=self._extract_joins(query_text),
                aggregations=self._extract_aggregations(query_text),
                where_conditions=self._extract_where_conditions(parsed),
                filter_columns=self._extract_filter_columns(parsed),
                cte_usage=self._has_cte(query_text),
                subqueries_count=self._count_subqueries(query_text),
                group_by_columns=self._extract_group_by(query_text),
                order_by_columns=self._extract_order_by(query_text),
                window_functions=self._extract_window_functions(query_text),
                run_quantity=run_quantity,
                execution_time=execution_time
            )
        except Exception as e:
            print(f"Error analyzing query {query_id}: {e}")
            return None

    def _get_query_type(self, query: str) -> str:
        """Определяет тип запроса"""
        query_upper = query.upper().strip()
        if query_upper.startswith('SELECT') or query_upper.startswith('WITH'):
            return 'SELECT'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        else:
            return 'UNKNOWN'

    def _extract_tables(self, query: str) -> Set[str]:
        """Извлекает названия таблиц из запроса"""
        tables = set()

        # Ищем паттерны FROM и JOIN
        from_pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)'
        matches = re.findall(from_pattern, query, re.IGNORECASE)

        for match in matches:
            # Убираем алиасы
            table_name = match.split()[0] if ' ' in match else match
            tables.add(table_name)

        return tables

    def _extract_joins(self, query: str) -> List[Dict]:
        """Извлекает информацию о JOIN'ах"""
        joins = []

        # Паттерн для различных типов JOIN'ов
        join_pattern = r'\b((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?:ON\s+(.+?)(?=\s+(?:JOIN|WHERE|GROUP|ORDER|LIMIT|$)))?'

        matches = re.findall(join_pattern, query, re.IGNORECASE | re.DOTALL)

        for match in matches:
            join_type, table, alias, condition = match
            joins.append({
                'type': join_type.strip(),
                'table': table,
                'alias': alias if alias else None,
                'condition': condition.strip() if condition else None
            })

        return joins

    def _extract_aggregations(self, query: str) -> List[str]:
        """Извлекает агрегатные функции"""
        aggregations = []
        query_upper = query.upper()

        for func in self.aggregate_functions:
            pattern = rf'\b{func.upper()}\s*\('
            if re.search(pattern, query_upper):
                aggregations.append(func)

        return aggregations

    def _extract_where_conditions(self, parsed) -> List[str]:
        """Извлекает условия WHERE"""
        conditions = []
        # Упрощенная реализация - можно расширить
        return conditions

    def _has_cte(self, query: str) -> bool:
        """Проверяет наличие CTE (Common Table Expression)"""
        return re.search(r'\bWITH\s+\w+\s+AS\s*\(', query, re.IGNORECASE) is not None

    def _count_subqueries(self, query: str) -> int:
        """Подсчитывает количество подзапросов"""
        # Упрощенный подсчет по количеству SELECT после основного
        select_count = len(re.findall(r'\bSELECT\b', query, re.IGNORECASE))
        return max(0, select_count - 1)  # Вычитаем основной SELECT

    def _extract_group_by(self, query: str) -> List[str]:
        """Извлекает колонки GROUP BY"""
        match = re.search(r'\bGROUP\s+BY\s+(.+?)(?=\s+(?:HAVING|ORDER|LIMIT|$))',
                          query, re.IGNORECASE | re.DOTALL)
        if match:
            group_by_text = match.group(1)
            return [col.strip() for col in group_by_text.split(',')]
        return []

    def _extract_order_by(self, query: str) -> List[str]:
        """Извлекает колонки ORDER BY"""
        match = re.search(r'\bORDER\s+BY\s+(.+?)(?=\s+(?:LIMIT|$))',
                          query, re.IGNORECASE | re.DOTALL)
        if match:
            order_by_text = match.group(1)
            return [col.strip() for col in order_by_text.split(',')]
        return []

    def _extract_window_functions(self, query: str) -> List[str]:
        """Извлекает оконные функции"""
        window_funcs = []
        query_upper = query.upper()

        for func in self.window_functions:
            pattern = rf'\b{func.upper()}\s*\(\s*\)\s+OVER\s*\('
            if re.search(pattern, query_upper):
                window_funcs.append(func)

        return window_funcs

    def _extract_filter_columns(self, parsed_statement) -> List[str]:
        """
        Recursively extracts column names from a WHERE clause.
        """
        filter_columns = []

        # 1. Find the WHERE clause token
        where_clause = None
        for token in parsed_statement.tokens:
            if isinstance(token, Where):
                where_clause = token
                break

        if not where_clause:
            return []

        # 2. Recursively process tokens within the WHERE clause
        def process_tokens(tokens):
            for token in tokens:
                if isinstance(token, Comparison):
                    for sub_token in token.tokens:
                        if isinstance(sub_token, Identifier):
                            filter_columns.append(sub_token.get_real_name())
                            break  # Found the column in this comparison

                elif isinstance(token, Function):
                    for sub_token in token.tokens:
                        if isinstance(sub_token, Parenthesis):
                            for param_token in sub_token.tokens:
                                if isinstance(param_token, Identifier):
                                    filter_columns.append(param_token.get_real_name())

                # Recursively dive into nested conditions (e.g., AND (col1 = 1 OR col2 = 2))
                elif hasattr(token, 'tokens'):
                    process_tokens(token.tokens)

        process_tokens(where_clause.tokens)
        return list(set(filter_columns))  # Return unique columns

    def get_query_statistics(self, patterns: List[QueryPattern]) -> Dict:
        """Собирает статистику по запросам"""
        stats = {
            'total_queries': len(patterns),
            'query_types': defaultdict(int),
            'most_used_tables': defaultdict(int),
            'join_patterns': defaultdict(int),
            'aggregation_usage': defaultdict(int),
            'most_used_filter_columns': defaultdict(int),
            'most_used_group_by_columns': defaultdict(int),
            'cte_usage': sum(1 for p in patterns if p.cte_usage),
            'avg_execution_time': sum(p.execution_time for p in patterns) / len(patterns) if patterns else 0,
            'high_frequency_queries': []
        }

        for pattern in patterns:
            stats['query_types'][pattern.query_type] += 1

            for table in pattern.tables_used:
                stats['most_used_tables'][table] += pattern.run_quantity

            # --- START OF NEW LOGIC ---
            for col in pattern.filter_columns:
                # We weight the usage by how often the query is run
                stats['most_used_filter_columns'][col] += pattern.run_quantity

            for col in pattern.group_by_columns:
                # Also weight this by run quantity
                stats['most_used_group_by_columns'][col] += pattern.run_quantity
            # --- END OF NEW LOGIC ---

            for join in pattern.joins:
                stats['join_patterns'][join['type']] += 1

            for agg in pattern.aggregations:
                stats['aggregation_usage'][agg] += 1

            if pattern.run_quantity > 100:  # Считаем часто выполняемыми
                stats['high_frequency_queries'].append({
                    'query_id': pattern.query_id,
                    'run_quantity': pattern.run_quantity,
                    'execution_time': pattern.execution_time
                })

        # Конвертируем defaultdict в обычные dict для сериализации
        for key in ['query_types', 'most_used_tables', 'join_patterns', 'aggregation_usage']:
            stats[key] = dict(stats[key])

        return stats