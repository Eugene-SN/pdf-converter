#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SSIM Calculator для PDF Converter Pipeline v4.0
Вспомогательный модуль для вычисления SSIM между изображениями
"""

import cv2
import numpy as np
from skimage.metrics import structural_similarity as ssim
from typing import Tuple, Optional
import structlog

logger = structlog.get_logger("ssim_calculator")


def calculate_ssim(
    img_path1: str, 
    img_path2: str, 
    resize_dim: Optional[Tuple[int, int]] = None,
    win_size: int = 7
) -> float:
    """
    Вычисление SSIM между двумя изображениями
    
    Args:
        img_path1: Путь к первому изображению
        img_path2: Путь ко второму изображению
        resize_dim: Размер для изменения изображений (опционально)
        win_size: Размер окна для SSIM
        
    Returns:
        float: SSIM score [0..1]
    """
    try:
        # Загружаем изображения
        img1 = cv2.imread(img_path1)
        img2 = cv2.imread(img_path2)
        
        if img1 is None or img2 is None:
            logger.error(f"Cannot load images: {img_path1}, {img_path2}")
            return 0.0
        
        # Изменяем размер если указано
        if resize_dim:
            img1 = cv2.resize(img1, resize_dim)
            img2 = cv2.resize(img2, resize_dim)
        
        # Приводим к одному размеру если они отличаются
        if img1.shape != img2.shape:
            height = min(img1.shape[0], img2.shape[0])
            width = min(img1.shape[1], img2.shape[1])
            img1 = cv2.resize(img1, (width, height))
            img2 = cv2.resize(img2, (width, height))
        
        # Конвертируем в grayscale
        gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
        
        # Вычисляем SSIM
        ssim_score = ssim(gray1, gray2, win_size=win_size)
        
        return float(ssim_score)
        
    except Exception as e:
        logger.error(f"Error calculating SSIM: {e}")
        return 0.0


def calculate_ssim_arrays(
    img1: np.ndarray, 
    img2: np.ndarray, 
    win_size: int = 7
) -> float:
    """
    Вычисление SSIM между двумя массивами изображений
    
    Args:
        img1: Первое изображение как numpy array
        img2: Второе изображение как numpy array
        win_size: Размер окна для SSIM
        
    Returns:
        float: SSIM score [0..1]
    """
    try:
        # Приводим к одному размеру если они отличаются
        if img1.shape != img2.shape:
            height = min(img1.shape[0], img2.shape[0])
            width = min(img1.shape[1], img2.shape[1])
            img1 = cv2.resize(img1, (width, height))
            img2 = cv2.resize(img2, (width, height))
        
        # Конвертируем в grayscale если нужно
        if len(img1.shape) == 3:
            img1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
        if len(img2.shape) == 3:
            img2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
        
        # Вычисляем SSIM
        ssim_score = ssim(img1, img2, win_size=win_size)
        
        return float(ssim_score)
        
    except Exception as e:
        logger.error(f"Error calculating SSIM from arrays: {e}")
        return 0.0


if __name__ == "__main__":
    # Пример использования
    print("SSIM Calculator module loaded successfully")
    
    # Тестовые пути (в реальности будут существующие файлы)
    # score = calculate_ssim("/path/to/image1.png", "/path/to/image2.png")
    # print(f"SSIM score: {score:.3f}")