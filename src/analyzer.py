from loguru import logger
from typing import Dict, Any
from src.ddl_parser import DDLParser
from src.query_analyzer import QueryAnalyzer
from src.db_stats_collector import DatabaseStatsCollector, safe_json_serialize


class DataAnalyzer:
    def __init__(self):
        self.ddl_parser = DDLParser()
        self.query_analyzer = QueryAnalyzer()
        self.db_collector = None

    def analyze_input_data(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Полный анализ входных данных"""

        logger.info("Starting data analysis...")

        analysis_result = {
            'schema_analysis': {},
            'query_analysis': {},
            'database_stats': {},
            'recommendations_input': {}
        }

        # 1. Анализ DDL
        logger.info("Analyzing DDL statements...")
        if 'ddl' in input_data:
            tables = self.ddl_parser.parse_ddl_statements(input_data['ddl'])
            schema_stats = self.ddl_parser.get_table_stats(tables)

            analysis_result['schema_analysis'] = {
                'tables': [
                    {
                        'full_name': f"{t.catalog}.{t.schema}.{t.name}",
                        'catalog': t.catalog,
                        'schema': t.schema,
                        'name': t.name,
                        'columns': [
                            {'name': c.name, 'type': c.data_type}
                            for c in t.columns
                        ]
                    }
                    for t in tables
                ],
                'statistics': schema_stats
            }

        # 2. Анализ запросов
        logger.info("Analyzing SQL queries...")
        if 'queries' in input_data:
            query_patterns = self.query_analyzer.analyze_queries(input_data['queries'])
            query_stats = self.query_analyzer.get_query_statistics(query_patterns)

            analysis_result['query_analysis'] = {
                'patterns': [
                    {
                        'query_id': p.query_id,
                        'type': p.query_type,
                        'tables_used': list(p.tables_used),
                        'joins': p.joins,
                        'aggregations': p.aggregations,
                        'cte_usage': p.cte_usage,
                        'run_quantity': p.run_quantity,
                        'execution_time': p.execution_time
                    }
                    for p in query_patterns
                ],
                'statistics': query_stats
            }

        # 3. Сбор статистики БД
        logger.info("Collecting database statistics...")
        if 'url' in input_data:
            try:
                self.db_collector = DatabaseStatsCollector(input_data['url'])
                if self.db_collector.connect():

                    # Получаем список таблиц из DDL анализа
                    table_names = []
                    if 'schema_analysis' in analysis_result and 'tables' in analysis_result['schema_analysis']:
                        table_names = [t['full_name'] for t in analysis_result['schema_analysis']['tables']]

                    # Собираем статистику
                    db_overview = self.db_collector.get_database_overview()
                    table_stats = self.db_collector.collect_table_statistics(table_names)

                    analysis_result['database_stats'] = {
                        'overview': db_overview,
                        'table_statistics': [
                            {
                                'table_name': ts.table_name,
                                'row_count': ts.row_count,
                                'size_bytes': ts.size_bytes,
                                'column_stats': safe_json_serialize(ts.column_stats)
                            }
                            for ts in table_stats
                        ]
                    }

                    self.db_collector.close()
                else:
                    analysis_result['database_stats'] = {'error': 'Failed to connect to database'}

            except Exception as e:
                logger.error(f"Database analysis failed: {e}")
                analysis_result['database_stats'] = {'error': str(e)}

        # 4. Подготовка данных для LLM
        analysis_result['recommendations_input'] = self._prepare_llm_input(analysis_result)

        logger.info("Analysis completed")
        return safe_json_serialize(analysis_result)

    def _prepare_llm_input(self, analysis: Dict) -> Dict:
        """Подготавливает структурированные данные для LLM"""

        llm_input = {
            'optimization_context': {
                'database_type': analysis.get('database_stats', {}).get('overview', {}).get('driver', 'unknown'),
                'total_tables': len(analysis.get('schema_analysis', {}).get('tables', [])),
                'total_queries': len(analysis.get('query_analysis', {}).get('patterns', []))
            },
            'performance_issues': [],
            'optimization_opportunities': [],
            'schema_structure': analysis.get('schema_analysis', {}),
            'query_patterns': analysis.get('query_analysis', {}),
            'data_statistics': analysis.get('database_stats', {})
        }

        # Выявление проблем производительности
        query_stats = analysis.get('query_analysis', {}).get('statistics', {})

        # Частые JOIN'ы как возможность для денормализации
        if query_stats.get('join_patterns', {}).get('JOIN', 0) > 0:
            llm_input['optimization_opportunities'].append({
                'type': 'denormalization',
                'description': 'Frequent joins detected - consider denormalization',
                'impact': 'high',
                'tables_involved': list(query_stats.get('most_used_tables', {}).keys())
            })

        # Высокая частота выполнения запросов
        high_freq_queries = query_stats.get('high_frequency_queries', [])
        if high_freq_queries:
            llm_input['performance_issues'].append({
                'type': 'high_frequency_queries',
                'description': f'{len(high_freq_queries)} queries with high execution frequency',
                'queries': high_freq_queries
            })

        return llm_input


if __name__ == "__main__":
    sample_input_file = "../data/flights.json"
    import json
    with open(sample_input_file, 'r', encoding='utf-8') as f:
        sample_input = json.load(f)

    analyzer = DataAnalyzer()
    result = analyzer.analyze_input_data(sample_input)

    print("Analysis completed!")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    with open("../backup/analysis_output.json", "w", encoding="utf-8") as out_f:
        json.dump(result, out_f, indent=2, ensure_ascii=False)