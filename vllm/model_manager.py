#!/usr/bin/env python3
"""
Model Manager для динамической подгрузки/выгрузки моделей в vLLM
PDF Converter Pipeline v2.0 - Исправленная версия
"""

import asyncio
import logging
import gc
import os
import time
from threading import Lock
from typing import Dict, Optional, Any
from dataclasses import dataclass
from enum import Enum

import torch

logger = logging.getLogger(__name__)


class ModelState(Enum):
    UNLOADED = "unloaded"
    LOADING = "loading"
    LOADED = "loaded"
    UNLOADING = "unloading"
    ERROR = "error"


class TaskType(Enum):
    CONTENT_TRANSFORMATION = "content_transformation"
    TRANSLATION = "translation"


@dataclass
class ModelConfig:
    name: str
    alias: str
    task_type: TaskType
    estimated_vram_gb: float
    tensor_parallel_size: int = 2
    max_model_len: int = 8192
    gpu_memory_utilization: float = 0.9


class DynamicModelManager:
    def __init__(self):
        self.models: Dict[str, ModelConfig] = {}
        self.current_model: Optional[str] = None
        self.model_states: Dict[str, ModelState] = {}
        self.vllm_engine = None
        self.lock = Lock()
        self._register_models()

    def _register_models(self):
        """Регистрация моделей из переменных окружения с корректными именами"""
        content_model_name = os.getenv("VLLM_CONTENT_MODEL", "Qwen/Qwen2.5-VL-32B-Instruct")
        translation_model_name = os.getenv("VLLM_TRANSLATION_MODEL", "Qwen/Qwen2.5-32B-Instruct")

        content_model = ModelConfig(
            name=content_model_name,
            alias="content-transformer",
            task_type=TaskType.CONTENT_TRANSFORMATION,
            estimated_vram_gb=32.0,
            tensor_parallel_size=2,
            max_model_len=8192,
            gpu_memory_utilization=0.9,
        )

        translation_model = ModelConfig(
            name=translation_model_name,
            alias="translator",
            task_type=TaskType.TRANSLATION,
            estimated_vram_gb=30.0,
            tensor_parallel_size=2,
            max_model_len=8192,
            gpu_memory_utilization=0.9,
        )

        self.models = {
            "content_transformation": content_model,
            "translation": translation_model,
        }

        for model_key in self.models.keys():
            self.model_states[model_key] = ModelState.UNLOADED

        logger.info(f"Зарегистрировано {len(self.models)} моделей")

    def get_available_vram_gb(self) -> float:
        """Получение доступной VRAM на всех GPU"""
        try:
            if not torch.cuda.is_available():
                return 0.0
            total_free = 0.0
            for i in range(torch.cuda.device_count()):
                torch.cuda.set_device(i)
                # Используем свободную память по данным CUDA
                free, total = torch.cuda.mem_get_info()
                total_free += free / (1024 ** 3)
            return round(total_free, 2)
        except Exception as e:
            logger.error(f"Ошибка определения VRAM: {e}")
            return 0.0

    def _cleanup_gpu_memory(self):
        """Принудительная очистка GPU памяти"""
        try:
            logger.info("Очистка GPU памяти...")
            if torch.cuda.is_available():
                for i in range(torch.cuda.device_count()):
                    torch.cuda.set_device(i)
                    torch.cuda.empty_cache()
                    torch.cuda.synchronize()
            for _ in range(3):
                gc.collect()
                time.sleep(0.2)
            logger.info("GPU память очищена")
        except Exception as e:
            logger.error(f"Ошибка очистки GPU памяти: {e}")

    async def unload_current_model(self) -> bool:
        """Выгрузка текущей модели"""
        if not self.current_model:
            return True
        with self.lock:
            try:
                logger.info(f"Выгружаем модель: {self.current_model}")
                self.model_states[self.current_model] = ModelState.UNLOADING
                if self.vllm_engine:
                    # Для vLLM нет graceful shutdown — освобождаем ссылки
                    del self.vllm_engine
                    self.vllm_engine = None
                self._cleanup_gpu_memory()
                self.model_states[self.current_model] = ModelState.UNLOADED
                self.current_model = None
                logger.info("Модель успешно выгружена")
                return True
            except Exception as e:
                logger.error(f"Ошибка выгрузки модели: {e}")
                return False

    async def load_model(self, model_key: str) -> bool:
        """Загрузка модели"""
        if model_key not in self.models:
            logger.error(f"Неизвестная модель: {model_key}")
            return False

        model_config = self.models[model_key]
        with self.lock:
            try:
                # Если модель уже загружена
                if self.current_model == model_key and self.model_states[model_key] == ModelState.LOADED:
                    logger.info(f"Модель {model_key} уже загружена")
                    return True

                logger.info(f"Загружаем модель: {model_config.name}")
                self.model_states[model_key] = ModelState.LOADING

                # Выгрузка предыдущей модели
                if self.current_model and self.current_model != model_key:
                    if not await self.unload_current_model():
                        logger.error("Не удалось выгрузить предыдущую модель")
                        return False

                # Создание vLLM engine
                from vllm import AsyncLLMEngine
                from vllm.engine.arg_utils import AsyncEngineArgs

                engine_args = AsyncEngineArgs(
                    model=model_config.name,
                    tensor_parallel_size=model_config.tensor_parallel_size,
                    gpu_memory_utilization=model_config.gpu_memory_utilization,
                    max_model_len=model_config.max_model_len,
                    dtype="bfloat16",
                    trust_remote_code=True,
                    enable_prefix_caching=True,
                    disable_log_stats=False,
                    download_dir=os.getenv("HF_HOME", "/models/huggingface"),
                )

                self.vllm_engine = AsyncLLMEngine.from_engine_args(engine_args)

                # Обновление состояния
                self.current_model = model_key
                self.model_states[model_key] = ModelState.LOADED
                logger.info(f"Модель {model_config.name} успешно загружена")
                return True

            except Exception as e:
                logger.error(f"Ошибка загрузки модели {model_key}: {e}")
                self.model_states[model_key] = ModelState.ERROR
                return False

    async def ensure_model_loaded(self, task_type: TaskType) -> bool:
        """Обеспечение загрузки нужной модели для задачи"""
        model_key = None
        for key, config in self.models.items():
            if config.task_type == task_type:
                model_key = key
                break
        if not model_key:
            logger.error(f"Модель для задачи {task_type} не найдена")
            return False

        if (self.current_model != model_key or
                self.model_states.get(model_key) != ModelState.LOADED):
            logger.info(f"Переключаемся на модель {model_key} для задачи {task_type.value}")
            return await self.load_model(model_key)
        return True

    def get_status(self) -> Dict[str, Any]:
        """Получение статуса менеджера"""
        return {
            "current_model": self.current_model,
            "model_states": {k: v.value for k, v in self.model_states.items()},
            "available_vram_gb": self.get_available_vram_gb(),
            "models_registered": len(self.models),
        }


# Глобальный экземпляр
model_manager = DynamicModelManager()


async def initialize_model_manager():
    """Инициализация менеджера моделей"""
    logger.info("Инициализация Dynamic Model Manager для PDF Converter Pipeline v2.0")
    # Пред-загружаем модель для content transformation
    success = await model_manager.load_model("content_transformation")
    if success:
        logger.info("Менеджер моделей готов к работе")
    else:
        logger.error("Ошибка инициализации менеджера моделей")
    return success
