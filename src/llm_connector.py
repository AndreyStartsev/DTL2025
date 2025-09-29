import os
from langchain_openai.chat_models import ChatOpenAI
from dotenv import load_dotenv, find_dotenv
from loguru import logger
from pydantic import BaseModel, Field


class DBOptimizationResponse(BaseModel):
    ddl: str = Field(..., description="The optimized DDL statements")
    migrations: str = Field(..., description="The data migration scripts")

class RewrittenQueries(BaseModel):
    queries: list[str] = Field(..., description="List of rewritten SQL queries")

load_dotenv(find_dotenv())

def get_llm(model_name: str) -> ChatOpenAI:
    """Initializes and returns a ChatOpenAI instance for OpenRouter."""
    return ChatOpenAI(
        openai_api_key=os.getenv("OPENROUTER_API_KEY", ""),
        openai_api_base="https://openrouter.ai/api/v1",
        model_name=model_name,
        max_tokens=10000,
        temperature=0.0,
        default_headers={
            "X-Title": "db enhancer",
        }
    )

def llm_call_with_so(model: ChatOpenAI, prompt: str, output_format: BaseModel) -> BaseModel:
    model = model.with_structured_output(output_format)
    response = model.invoke(prompt)
    return response

# Example usage:
if __name__ == "__main__":
    model_name = "google/gemini-2.5-flash"
    llm = get_llm(model_name)
    logger.success(f"Initialized LLM with model: {model_name}")

    import json
    from time import time
    from prompts import PROMPT_STEP1, PROMPT_STEP2
    from analyzer import DataAnalyzer
    from report_creator import create_optimization_report

    input_data_path = "../data/flights.json"
    with open(input_data_path, 'r', encoding='utf-8') as f:
        input_data = json.load(f)
    ddl = input_data.get('ddl', '')
    queries = input_data.get('queries', '')

    start_time = time()
    analyzer = DataAnalyzer()
    result = analyzer.analyze_input_data(input_data)
    db_analysis = create_optimization_report(result)

    prompt =PROMPT_STEP1.format(
        db_analysis=json.dumps(db_analysis, ensure_ascii=False, indent=2),
        ddl=ddl,)
    response = llm_call_with_so(llm, prompt, DBOptimizationResponse)

    logger.success(f"Optimization of DDL and Migration Scripts completed in {time() - start_time:.2f} seconds")
    print("\033[094mOptimized DDL and Migration Scripts:\033[0m")
    print(response.ddl)
    print("\n" + "=" * 60 + "\n")
    print(response.migrations)
    print("\n" + "=" * 60 + "\n")

    start_time = time()
    prompt = PROMPT_STEP2.format(
        original_queries=queries,
        original_ddl=ddl,
        new_ddl=response.model_dump().get('ddl', '')
    )
    response = llm_call_with_so(llm, prompt, RewrittenQueries)

    logger.success(f"Rewritten Queries completed in {time() - start_time:.2f} seconds")
    print("\033[094mRewritten Queries:\033[0m")
    for query in response.queries:
        print(query)