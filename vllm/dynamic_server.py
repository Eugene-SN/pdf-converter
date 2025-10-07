#!/usr/bin/env python3
"""
Динамический vLLM сервер с автоматической сменой моделей
PDF Converter Pipeline v2.0 - Исправленная версия
"""

import asyncio
import uvicorn
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from model_manager import model_manager, TaskType, initialize_model_manager

MAX_CONCURRENT_REQUESTS = max(1, int(os.getenv('VLLM_MAX_CONCURRENT', '2')))
QUEUE_TIMEOUT_SECONDS = float(os.getenv('VLLM_QUEUE_TIMEOUT_SECONDS', '10'))
MAX_ALLOWED_TOKENS = int(os.getenv('VLLM_MAX_TOKENS', '4096'))
MAX_ALLOWED_TEMPERATURE = float(os.getenv('VLLM_MAX_TEMPERATURE', '0.8'))

_REQUEST_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Pydantic модели
class ChatMessage(BaseModel):
    role: str = Field(..., description="Роль: system, user, assistant")
    content: str = Field(..., description="Содержание сообщения")

class ChatCompletionRequest(BaseModel):
    model: str = Field(..., description="Название модели")
    messages: List[ChatMessage] = Field(..., description="Список сообщений")
    temperature: float = Field(0.1, ge=0.0, le=2.0)
    max_tokens: int = Field(4096, ge=1, le=32768)
    top_p: float = Field(0.9, ge=0.0, le=1.0)
    top_k: int = Field(50, ge=1, le=100)
    stream: bool = Field(False)
    task_type: Optional[str] = Field(None, description="Тип задачи для выбора модели")

class ModelSwapRequest(BaseModel):
    model_key: str = Field(..., description="Ключ модели для загрузки")

# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Запуск Dynamic vLLM Server для PDF Converter Pipeline v2.0")
    success = await initialize_model_manager()
    if not success:
        logger.error("❌ Ошибка инициализации Model Manager")
        raise RuntimeError("Не удалось инициализировать Model Manager")
    logger.info("✅ Dynamic vLLM Server готов к работе")
    yield
    # Shutdown
    logger.info("🔄 Остановка Dynamic vLLM Server")
    if model_manager.current_model:
        await model_manager.unload_current_model()
    logger.info("✅ Dynamic vLLM Server остановлен")

# FastAPI app
app = FastAPI(
    title="Dynamic vLLM Server",
    description="vLLM сервер с динамической подгрузкой моделей",
    version="2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def determine_task_type_from_messages(messages: List[ChatMessage]) -> TaskType:
    """Определение типа задачи по содержанию сообщений"""
    try:
        combined_text = " ".join([msg.content.lower() for msg in messages])
        content_keywords = [
            "преобразуй", "markdown", "структура", "таблица",
            "pdf", "документ", "извлечение", "форматирование",
        ]
        translation_keywords = [
            "переведи", "translate", "перевод", "translation",
            "русский", "english", "中文", "язык", "language",
        ]
        content_score = sum(1 for keyword in content_keywords if keyword in combined_text)
        translation_score = sum(1 for keyword in translation_keywords if keyword in combined_text)
        return TaskType.TRANSLATION if translation_score > content_score else TaskType.CONTENT_TRANSFORMATION
    except Exception as e:
        logger.warning(f"Ошибка определения типа задачи: {e}")
        return TaskType.CONTENT_TRANSFORMATION

@app.post("/v1/chat/completions")
async def create_chat_completion(request: ChatCompletionRequest):
    """OpenAI-совместимый endpoint с автоматической сменой моделей"""
    try:
        start_time = time.time()

        if request.stream:
            raise HTTPException(status_code=400, detail="Streaming responses не поддерживаются")

        # Определение типа задачи
        task_type: Optional[TaskType] = None
        if request.task_type:
            try:
                task_type = TaskType(request.task_type)
            except ValueError:
                task_type = None
        if not task_type:
            task_type = determine_task_type_from_messages(request.messages)
        logger.info(f"📝 Запрос обработки. Тип задачи: {task_type.value}")

        # Автоматическая загрузка нужной модели
        model_ready = await model_manager.ensure_model_loaded(task_type)
        if not model_ready:
            raise HTTPException(
                status_code=503,
                detail=f"Не удалось загрузить модель для задачи {task_type.value}",
            )

        queue_acquired = False
        queue_start = time.time()
        try:
            await asyncio.wait_for(_REQUEST_SEMAPHORE.acquire(), timeout=QUEUE_TIMEOUT_SECONDS)
            queue_acquired = True
        except asyncio.TimeoutError:
            raise HTTPException(status_code=503, detail="vLLM занят, попробуйте позже")

        queue_wait = time.time() - queue_start
        concurrency_in_use = MAX_CONCURRENT_REQUESTS - _REQUEST_SEMAPHORE._value

        # Проверка vLLM engine
        if not model_manager.vllm_engine:
            raise HTTPException(status_code=503, detail="vLLM engine недоступен")

        # Подготовка промпта (формат Qwen Chat)
        prompt_parts: List[str] = []
        for message in request.messages:
            if message.role == "system":
                prompt_parts.append(f"<|im_start|>system\n{message.content}<|im_end|>")
            elif message.role == "user":
                prompt_parts.append(f"<|im_start|>user\n{message.content}<|im_end|>")
            elif message.role == "assistant":
                prompt_parts.append(f"<|im_start|>assistant\n{message.content}<|im_end|>")
        prompt_parts.append("<|im_start|>assistant\n")
        formatted_prompt = "\n".join(prompt_parts)

        # Параметры генерации
        from vllm import SamplingParams
        from vllm.utils import random_uuid

        effective_max_tokens = max(1, min(request.max_tokens, MAX_ALLOWED_TOKENS))
        effective_temperature = max(0.0, min(request.temperature, MAX_ALLOWED_TEMPERATURE))

        sampling_params = SamplingParams(
            temperature=effective_temperature,
            max_tokens=effective_max_tokens,
            top_p=request.top_p,
            top_k=request.top_k,
            stop=["<|im_end|>"],
        )

        # Генерация ответа
        request_id = random_uuid()
        results = model_manager.vllm_engine.generate(
            formatted_prompt,
            sampling_params,
            request_id=request_id,
        )

        # Ожидание результата
        final_output = None
        async for request_output in results:
            final_output = request_output

        if final_output is None or not final_output.outputs:
            raise HTTPException(status_code=500, detail="No output generated")

        # ИСПРАВЛЕНО: правильное обращение к полям vLLM RequestOutput
        generated_text = (final_output.outputs[0].text or "").strip()
        
        # ИСПРАВЛЕНО: prompt_token_ids - это список, не массив объектов
        prompt_tokens = len(final_output.prompt_token_ids or [])
        
        # ИСПРАВЛЕНО: безопасное получение token_ids из CompletionOutput
        try:
            completion_tokens = len(final_output.outputs[0].token_ids or [])
        except (AttributeError, TypeError):
            # Fallback если token_ids недоступен
            completion_tokens = max(1, len(generated_text.split()) // 4)  # приблизительная оценка
            
        total_tokens = prompt_tokens + completion_tokens
        processing_time = time.time() - start_time

        # Формирование OpenAI-совместимого ответа
        response = {
            "id": f"chatcmpl-{request_id}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model_manager.models[model_manager.current_model].name,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": generated_text
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens
            },
            "pdf_converter_meta": {
                "task_type": task_type.value,
                "model_key": model_manager.current_model,
                "processing_time_seconds": round(processing_time, 2),
                "vram_usage_gb": round(max(0.0, 48.0 - model_manager.get_available_vram_gb()), 1),
                "queue_wait_seconds": round(queue_wait, 3),
                "concurrency_in_use": concurrency_in_use,
                "max_concurrent": MAX_CONCURRENT_REQUESTS,
                "max_tokens_effective": effective_max_tokens,
            }
        }

        logger.info(
            "✅ Ответ сгенерирован за %.2fs (ждали %.2fs). Токенов: %d",
            processing_time,
            queue_wait,
            total_tokens,
        )
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка генерации: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка генерации: {str(e)}")
    finally:
        if '_REQUEST_SEMAPHORE' in globals():
            # освобождаем семафор только если он был захвачен
            if 'queue_acquired' in locals() and queue_acquired:
                _REQUEST_SEMAPHORE.release()

@app.post("/v1/models/swap")
async def swap_model(request: ModelSwapRequest):
    """Принудительная смена модели"""
    try:
        logger.info(f"🔄 Запрос смены модели на: {request.model_key}")
        if request.model_key not in model_manager.models:
            available_models = list(model_manager.models.keys())
            raise HTTPException(
                status_code=400,
                detail=f"Неизвестная модель: {request.model_key}. Доступны: {available_models}",
            )
        success = await model_manager.load_model(request.model_key)
        if success:
            return {
                "status": "success",
                "message": f"Модель {request.model_key} успешно загружена",
                "current_model": model_manager.current_model,
                "vram_available": model_manager.get_available_vram_gb(),
            }
        else:
            raise HTTPException(
                status_code=503,
                detail=f"Не удалось загрузить модель: {request.model_key}",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка смены модели: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/models")
async def list_models():
    """Список доступных моделей"""
    models_info: Dict[str, Dict[str, Any]] = {}
    for model_key, config in model_manager.models.items():
        models_info[model_key] = {
            "name": config.name,
            "alias": config.alias,
            "task_type": config.task_type.value,
            "state": model_manager.model_states[model_key].value,
        }

    models_list: List[Dict[str, Any]] = []
    for model_key, info in models_info.items():
        models_list.append({
            "id": info["name"],
            "object": "model",
            "created": int(time.time()),
            "owned_by": "pdf-converter-v2",
            "pdf_converter_meta": {
                "key": model_key,
                "alias": info["alias"],
                "task_type": info["task_type"],
                "state": info["state"],
            },
        })
    return {"object": "list", "data": models_list}

@app.get("/v1/models/status")
async def models_status():
    """Детальный статус системы моделей"""
    return {
        "manager_status": model_manager.get_status(),
        "system_info": {
            "total_vram_gb": 96.0,  # 2x A6000
            "available_vram_gb": model_manager.get_available_vram_gb(),
            "gpu_count": 2,
            "gpu_type": "NVIDIA A6000",
        },
    }

@app.get("/health")
async def health_check():
    """Проверка состояния сервера"""
    status = model_manager.get_status()
    is_healthy = (
        model_manager.current_model is not None and
        status["available_vram_gb"] > 2.0
    )
    response = {
        "status": "healthy" if is_healthy else "unhealthy",
        "timestamp": time.time(),
        "current_model": status["current_model"],
        "available_vram_gb": status["available_vram_gb"],
        "version": "v2.0-dynamic",
    }
    return JSONResponse(
        content=response,
        status_code=200 if is_healthy else 503,
    )

@app.get("/metrics")
async def metrics():
    """Метрики для Prometheus"""
    status = model_manager.get_status()
    metrics_lines = [
        "# HELP vllm_available_vram_gb Available VRAM in GB",
        "# TYPE vllm_available_vram_gb gauge",
        f'vllm_available_vram_gb {status["available_vram_gb"]}',
        "",
        "# HELP vllm_model_loaded Current model loaded (1=loaded, 0=not loaded)",
        "# TYPE vllm_model_loaded gauge",
    ]
    for model_key, config in model_manager.models.items():
        is_loaded = 1 if model_manager.model_states[model_key].name == "LOADED" else 0
        metrics_lines.append(
            f'vllm_model_loaded{{model_key="{model_key}",model_name="{config.name}"}} {is_loaded}'
        )
    return Response(
        content="\n".join(metrics_lines),
        media_type="text/plain",
    )

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    logger.info(f"🚀 Запуск Dynamic vLLM Server на {host}:{port}")
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=True,
        workers=1,  # важно: 1 worker для GPU
        reload=False,
    )
