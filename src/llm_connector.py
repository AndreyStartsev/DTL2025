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

def get_llm(model_name: str, max_tokens=16000, provider="openrouter") -> ChatOpenAI:
    """Initializes and returns a ChatOpenAI instance for OpenRouter."""
    if provider == "ollama":
        ollama_host = os.getenv("OLLAMA_HOST", "212.111.86.90")
        ollama_port = os.getenv("OLLAMA_PORT", "11434")
        print(f"🟣🟠🟡🟢🔵Using Ollama LLM at {ollama_host}:{ollama_port}")
        return ChatOpenAI(
            openai_api_key="ollama",  # Required but ignored by Ollama
            openai_api_base=f"http://{ollama_host}:{ollama_port}/v1",
            model_name="llama3.1:8b",
            max_tokens=max_tokens,
            temperature=0.0,
        )

    return ChatOpenAI(
        openai_api_key=os.getenv("OPENROUTER_API_KEY", ""),
        openai_api_base="https://openrouter.ai/api/v1",
        model_name=model_name,
        max_tokens=max_tokens,
        temperature=0.0,
        default_headers={
            "X-Title": "db enhancer",
        }
    )

def llm_call_with_so(model: ChatOpenAI, prompt: str, output_format: BaseModel) -> BaseModel:
    model = model.with_structured_output(output_format)
    response = model.invoke(prompt)
    return response

def llm_call_with_so_and_fallback(model: ChatOpenAI, prompt: str, output_format: BaseModel,
                                  num_retries: int = 5,
                                  fallback_model_id="google/gemini-2.5-flash",
                                  provider="openrouter") -> BaseModel:
    for attempt in range(num_retries):
        # for last attempt use fallback model
        if attempt == num_retries - 2:
            # save prompt to a file for debugging
            os.makedirs("./logs", exist_ok=True)
            with open(f"./logs/last_prompt_after_{attempt}_tries.txt", "w", encoding="utf-8") as f:
                f.write(prompt)
            # switch to fallback model
            logger.warning("Using fallback model for the last attempt.")
            model = get_llm(fallback_model_id, max_tokens=28000)
            test_call = model.invoke(prompt)
            logger.info(f"Fallback model response (truncated): {test_call}...")
        try:
            return llm_call_with_so(model, prompt, output_format,)
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
    raise ValueError(f"All {num_retries} attempts failed for LLM call.")

# Example usage:
if __name__ == "__main__":
    model_name = "meta-llama/llama-4-maverick"
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