# query_analyzer.py
import re
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Function, Parenthesis
from typing import List, Dict, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class QueryPattern:
    query_id: str
    query_type: str
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
        """Analyzes list of SQL queries"""
        patterns = []

        for query_data in queries:
            pattern = self._analyze_single_query(query_data)
            if pattern:
                patterns.append(pattern)

        return patterns

    def _analyze_single_query(self, query_data: Dict) -> QueryPattern:
        """Analyzes a single SQL query"""
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
        """Determines query type"""
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
        """Extracts table names from query"""
        tables = set()
        from_pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)'
        matches = re.findall(from_pattern, query, re.IGNORECASE)

        for match in matches:
            table_name = match.split()[0] if ' ' in match else match
            tables.add(table_name)

        return tables

    def _extract_joins(self, query: str) -> List[Dict]:
        """Extracts JOIN information"""
        joins = []
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
        """Extracts aggregate functions"""
        aggregations = []
        query_upper = query.upper()

        for func in self.aggregate_functions:
            pattern = rf'\b{func.upper()}\s*\('
            if re.search(pattern, query_upper):
                aggregations.append(func)

        return aggregations

    def _extract_where_conditions(self, parsed) -> List[str]:
        """Extracts WHERE conditions"""
        conditions = []
        return conditions

    def _has_cte(self, query: str) -> bool:
        """Checks for CTE (Common Table Expression)"""
        return re.search(r'\bWITH\s+\w+\s+AS\s*\(', query, re.IGNORECASE) is not None

    def _count_subqueries(self, query: str) -> int:
        """Counts subqueries"""
        select_count = len(re.findall(r'\bSELECT\b', query, re.IGNORECASE))
        return max(0, select_count - 1)

    def _extract_group_by(self, query: str) -> List[str]:
        """Extracts GROUP BY columns"""
        match = re.search(r'\bGROUP\s+BY\s+(.+?)(?=\s+(?:HAVING|ORDER|LIMIT|$))',
                          query, re.IGNORECASE | re.DOTALL)
        if match:
            group_by_text = match.group(1)
            return [col.strip() for col in group_by_text.split(',')]
        return []

    def _extract_order_by(self, query: str) -> List[str]:
        """Extracts ORDER BY columns"""
        match = re.search(r'\bORDER\s+BY\s+(.+?)(?=\s+(?:LIMIT|$))',
                          query, re.IGNORECASE | re.DOTALL)
        if match:
            order_by_text = match.group(1)
            return [col.strip() for col in order_by_text.split(',')]
        return []

    def _extract_window_functions(self, query: str) -> List[str]:
        """Extracts window functions"""
        window_funcs = []
        query_upper = query.upper()

        for func in self.window_functions:
            pattern = rf'\b{func.upper()}\s*\(\s*\)\s+OVER\s*\('
            if re.search(pattern, query_upper):
                window_funcs.append(func)

        return window_funcs

    def _extract_filter_columns(self, parsed_statement) -> List[str]:
        """Recursively extracts column names from WHERE clause"""
        filter_columns = []

        where_clause = None
        for token in parsed_statement.tokens:
            if isinstance(token, Where):
                where_clause = token
                break

        if not where_clause:
            return []

        def process_tokens(tokens):
            for token in tokens:
                if isinstance(token, Comparison):
                    for sub_token in token.tokens:
                        if isinstance(sub_token, Identifier):
                            filter_columns.append(sub_token.get_real_name())
                            break

                elif isinstance(token, Function):
                    for sub_token in token.tokens:
                        if isinstance(sub_token, Parenthesis):
                            for param_token in sub_token.tokens:
                                if isinstance(param_token, Identifier):
                                    filter_columns.append(param_token.get_real_name())

                elif hasattr(token, 'tokens'):
                    process_tokens(token.tokens)

        process_tokens(where_clause.tokens)
        return list(set(filter_columns))

    def get_query_statistics(self, patterns: List[QueryPattern]) -> Dict:
        """Collects query statistics"""
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

            for col in pattern.filter_columns:
                stats['most_used_filter_columns'][col] += pattern.run_quantity

            for col in pattern.group_by_columns:
                stats['most_used_group_by_columns'][col] += pattern.run_quantity

            for join in pattern.joins:
                stats['join_patterns'][join['type']] += 1

            for agg in pattern.aggregations:
                stats['aggregation_usage'][agg] += 1

            if pattern.run_quantity > 100:
                stats['high_frequency_queries'].append({
                    'query_id': pattern.query_id,
                    'run_quantity': pattern.run_quantity,
                    'execution_time': pattern.execution_time
                })

        # Convert defaultdict to dict
        for key in ['query_types', 'most_used_tables', 'join_patterns', 'aggregation_usage',
                    'most_used_filter_columns', 'most_used_group_by_columns']:
            stats[key] = dict(stats[key])

        return stats

    def identify_dimension_candidates(self, stats: Dict, schema_columns: List[Dict],
                                      threshold: int = 1000) -> List[Dict]:
        """
        Dynamically identifies dimension table candidates based on GROUP BY usage.
        Works for ANY database schema by analyzing column patterns.

        Args:
            stats: Query statistics with group_by column usage
            schema_columns: List of column definitions from schema
            threshold: Minimum usage count to consider a column

        Returns:
            List of dimension candidates with clustered related columns
        """
        group_by_cols = stats.get('most_used_group_by_columns', {})

        # Filter by threshold
        high_usage_cols = {col: count for col, count in group_by_cols.items()
                           if count >= threshold}

        if not high_usage_cols:
            return []

        # Dynamically cluster columns by similarity
        clusters = self._cluster_columns_by_similarity(high_usage_cols, schema_columns)

        # Convert clusters to dimension candidates
        dimension_candidates = []
        for cluster_name, columns in clusters.items():
            if columns:
                dimension_candidates.append({
                    'dimension_name': cluster_name,
                    'columns': [{'column': col, 'usage_count': count}
                                for col, count in columns.items()],
                    'total_usage': sum(columns.values())
                })

        return sorted(dimension_candidates, key=lambda x: x['total_usage'], reverse=True)

    def _cluster_columns_by_similarity(self, columns: Dict[str, int],
                                       schema_columns: List[Dict]) -> Dict[str, Dict[str, int]]:
        """
        Clusters columns by analyzing prefixes, suffixes, and semantic patterns.
        Completely data-driven - no hardcoded business logic.

        Returns: Dict[cluster_name, Dict[column_name, usage_count]]
        """
        clusters = defaultdict(lambda: {})
        unclustered = {}

        # 1. Group by common prefixes (e.g., "customer_", "product_", "order_")
        prefix_groups = self._group_by_prefix(columns)

        # 2. Group by semantic type (dates, IDs, flags, etc.)
        semantic_groups = self._group_by_semantic_type(columns, schema_columns)

        # 3. Merge strategies
        for col, count in columns.items():
            assigned = False

            # Try prefix grouping first (stronger signal)
            for prefix, group_cols in prefix_groups.items():
                if col in group_cols:
                    clusters[prefix][col] = count
                    assigned = True
                    break

            # If not assigned, try semantic grouping
            if not assigned:
                for sem_type, group_cols in semantic_groups.items():
                    if col in group_cols:
                        clusters[sem_type][col] = count
                        assigned = True
                        break

            # If still not assigned, it's a standalone dimension
            if not assigned:
                unclustered[col] = count

        # Add unclustered columns as individual dimensions
        for col, count in unclustered.items():
            clusters[col][col] = count

        return dict(clusters)

    def _group_by_prefix(self, columns: Dict[str, int]) -> Dict[str, List[str]]:
        """
        Groups columns that share common prefixes.
        Example: "customer_id", "customer_name" -> prefix "customer"
        """
        prefix_groups = defaultdict(list)

        for col in columns.keys():
            # Try to extract prefix (before underscore or camelCase boundary)
            parts = re.split(r'[_\s]', col.lower())

            if len(parts) > 1:
                # Multi-word column (e.g., "customer_name")
                prefix = parts[0]
                if len(prefix) > 2:  # Avoid very short prefixes
                    prefix_groups[prefix].append(col)
            else:
                # Try camelCase split (e.g., "CustomerName" -> "Customer")
                camel_match = re.match(r'^([A-Z][a-z]+)', col)
                if camel_match:
                    prefix = camel_match.group(1).lower()
                    prefix_groups[prefix].append(col)

        # Only keep prefixes with 2+ columns
        return {prefix: cols for prefix, cols in prefix_groups.items() if len(cols) >= 2}

    def _group_by_semantic_type(self, columns: Dict[str, int],
                                schema_columns: List[Dict]) -> Dict[str, List[str]]:
        """
        Groups columns by semantic meaning using pattern matching.
        Detects: dates, locations, identifiers, categories, etc.
        """
        semantic_groups = {
            'temporal': [],
            'geographic': [],
            'identifier': [],
            'categorical': [],
            'numeric': []
        }

        # Build column type lookup
        col_types = {col['name']: col.get('type', '').lower()
                     for col in schema_columns if 'name' in col}

        for col in columns.keys():
            col_lower = col.lower()
            col_type = col_types.get(col, '').lower()

            # Temporal patterns
            if any(pattern in col_lower for pattern in
                   ['date', 'time', 'year', 'month', 'day', 'quarter', 'timestamp', 'created', 'updated']):
                semantic_groups['temporal'].append(col)

            # Geographic patterns
            elif any(pattern in col_lower for pattern in
                     ['city', 'state', 'country', 'region', 'location', 'address', 'zip', 'postal']):
                semantic_groups['geographic'].append(col)

            # Identifier patterns (IDs, codes, keys)
            elif any(pattern in col_lower for pattern in
                     ['id', 'key', 'code', 'number']) or col_lower.endswith('_id'):
                semantic_groups['identifier'].append(col)

            # Categorical (low cardinality text fields)
            elif 'varchar' in col_type or 'char' in col_type or 'string' in col_type:
                semantic_groups['categorical'].append(col)

            # Numeric (but not IDs)
            elif any(t in col_type for t in ['int', 'decimal', 'float', 'double', 'numeric']) \
                    and 'id' not in col_lower:
                semantic_groups['numeric'].append(col)

        # Only return groups with 2+ columns
        return {group_type: cols for group_type, cols in semantic_groups.items()
                if len(cols) >= 2}

    def detect_schema_archetype(self, schema_info: Dict, query_stats: Dict) -> str:
        """
        Detects schema archetype based on structure and query patterns.

        Returns: 'single_big_table', 'star_schema', 'normalized_multitable', 'denormalized_multitable'
        """
        num_tables = schema_info.get('total_tables', 0)
        total_joins = sum(query_stats.get('join_patterns', {}).values())
        total_queries = query_stats.get('total_queries', 1)

        if num_tables == 1:
            return 'single_big_table'
        elif num_tables > 1 and total_joins > (total_queries / 2):
            # Many joins suggest normalized schema
            return 'normalized_multitable'
        elif num_tables > 1:
            return 'denormalized_multitable'
        else:
            return 'unknown'