# Airflow DAGs – ключевые параметры

- Переменная окружения `VLLM_MODEL_NAME` централизованно определяет модель vLLM для всех задач (`Qwen/Qwen3-30B-A3B-Instruct-2507` по умолчанию).
- При обновлении модели убедитесь, что переменная синхронизирована между Airflow, translator service и Quality Assurance авто-корректором.
- Переменная `TRANSLATOR_URL` должна быть передана во все компоненты Airflow (`webserver`, `scheduler`, `workers`, `triggerer`). При деплое в Kubernetes задайте её через общий `env` блок (например, Helm `values.yaml` верхнего уровня) с URL сервиса перевода `http://translator:8003`.
