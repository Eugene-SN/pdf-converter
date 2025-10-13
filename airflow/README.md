# Airflow DAGs – ключевые параметры

- Переменная окружения `VLLM_MODEL_NAME` централизованно определяет модель vLLM для всех задач (`Qwen/Qwen3-30B-A3B-Instruct-2507` по умолчанию).
- При обновлении модели убедитесь, что переменная синхронизирована между Airflow, translator service и Quality Assurance авто-корректором.
