#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AST Comparator для PDF Converter Pipeline v4.0
Сравнение структуры документа (AST) для проверки сохранения иерархии
"""

import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Union
import json
from dataclasses import dataclass
from datetime import datetime

# NLP и семантическое сравнение
from sentence_transformers import SentenceTransformer, util
import torch

# Утилиты для сравнения текста
import difflib
from textdistance import levenshtein, jaccard

# Утилиты
import structlog
from prometheus_client import Counter, Histogram, Gauge

# =======================================================================================
# КОНФИГУРАЦИЯ И МЕТРИКИ
# =======================================================================================

logger = structlog.get_logger("ast_comparator")

# Prometheus метрики
ast_comparison_requests = Counter('ast_comparison_requests_total', 'AST comparison requests', ['status'])
ast_comparison_duration = Histogram('ast_comparison_duration_seconds', 'AST comparison duration')
ast_similarity_score = Histogram('ast_similarity_score', 'AST structural similarity score')

@dataclass
class ASTComparisonConfig:
    """Конфигурация сравнения AST"""
    # Модель для семантического анализа
    semantic_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    
    # Веса для разных типов сравнения
    structural_weight: float = 0.7
    semantic_weight: float = 0.3
    
    # Пороги
    similarity_threshold: float = 0.9
    min_node_similarity: float = 0.8
    
    # Директории
    cache_dir: str = "/app/cache"
    models_dir: str = "/mnt/storage/models/shared/qa"

@dataclass
class ASTComparisonResult:
    """Результат сравнения AST"""
    overall_similarity: float
    structural_similarity: float
    semantic_similarity: float
    node_comparisons: List[Dict[str, Any]]
    issues_found: List[str]
    recommendations: List[str]
    processing_time: float
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

# =======================================================================================
# AST COMPARATOR КЛАСС
# =======================================================================================

class ASTComparator:
    """Класс для сравнения AST структур документов"""
    
    def __init__(self, config: Optional[ASTComparisonConfig] = None):
        self.config = config or ASTComparisonConfig()
        self.logger = structlog.get_logger("ast_comparator")
        
        # Инициализируем модель для семантического анализа
        self._initialize_semantic_model()
    
    def _initialize_semantic_model(self):
        """Инициализация модели sentence transformers"""
        try:
            self.semantic_model = SentenceTransformer(
                self.config.semantic_model_name,
                cache_folder=self.config.models_dir
            )
            self.logger.info(f"Semantic model loaded: {self.config.semantic_model_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to load semantic model: {e}")
            self.semantic_model = None
    
    async def compare_ast_structures(
        self,
        original_ast: Dict[str, Any],
        result_ast: Dict[str, Any],
        comparison_id: str
    ) -> ASTComparisonResult:
        """
        Основное сравнение AST структур
        
        Args:
            original_ast: AST оригинального документа
            result_ast: AST результирующего документа
            comparison_id: Идентификатор сравнения
            
        Returns:
            ASTComparisonResult: Результат сравнения
        """
        start_time = datetime.now()
        
        try:
            ast_comparison_requests.labels(status='started').inc()
            
            # Извлекаем плоские списки заголовков
            original_headers = self._flatten_ast(original_ast)
            result_headers = self._flatten_ast(result_ast)
            
            # Структурное сравнение
            structural_sim = await self._calculate_structural_similarity(
                original_headers, result_headers
            )
            
            # Семантическое сравнение
            semantic_sim = await self._calculate_semantic_similarity(
                original_headers, result_headers
            )
            
            # Детальное сравнение узлов
            node_comparisons = await self._compare_individual_nodes(
                original_headers, result_headers
            )
            
            # Общий скор
            overall_similarity = (
                structural_sim * self.config.structural_weight + 
                semantic_sim * self.config.semantic_weight
            )
            
            # Анализ проблем
            issues_found, recommendations = self._analyze_issues(
                original_headers, result_headers, node_comparisons, overall_similarity
            )
            
            # Обновляем метрики
            processing_time = (datetime.now() - start_time).total_seconds()
            ast_comparison_duration.observe(processing_time)
            ast_similarity_score.observe(overall_similarity)
            ast_comparison_requests.labels(status='success').inc()
            
            result = ASTComparisonResult(
                overall_similarity=overall_similarity,
                structural_similarity=structural_sim,
                semantic_similarity=semantic_sim,
                node_comparisons=node_comparisons,
                issues_found=issues_found,
                recommendations=recommendations,
                processing_time=processing_time,
                metadata={
                    "comparison_id": comparison_id,
                    "original_nodes_count": len(original_headers),
                    "result_nodes_count": len(result_headers),
                    "threshold": self.config.similarity_threshold,
                    "passed": overall_similarity >= self.config.similarity_threshold
                }
            )
            
            self.logger.info(
                f"AST comparison completed",
                comparison_id=comparison_id,
                similarity=overall_similarity,
                structural=structural_sim,
                semantic=semantic_sim,
                processing_time=processing_time
            )
            
            return result
            
        except Exception as e:
            ast_comparison_requests.labels(status='error').inc()
            self.logger.error(f"AST comparison error: {e}")
            raise
    
    def _flatten_ast(self, ast_node: Dict[str, Any], level: int = 1) -> List[Dict[str, Any]]:
        """Преобразование дерева AST в плоский список узлов"""
        nodes = []
        
        def traverse(node, current_level):
            # Добавляем текущий узел
            node_info = {
                "title": node.get("title", ""),
                "level": current_level,
                "full_text": "#" * current_level + " " + node.get("title", ""),
                "has_children": len(node.get("children", [])) > 0,
                "child_count": len(node.get("children", []))
            }
            nodes.append(node_info)
            
            # Рекурсивно обрабатываем детей
            for child in node.get("children", []):
                traverse(child, current_level + 1)
        
        if ast_node:
            traverse(ast_node, level)
        
        return nodes
    
    async def _calculate_structural_similarity(
        self,
        original_nodes: List[Dict[str, Any]],
        result_nodes: List[Dict[str, Any]]
    ) -> float:
        """Расчет структурного сходства"""
        try:
            if not original_nodes or not result_nodes:
                return 0.0
            
            # Сравниваем структуру по уровням
            original_levels = [node["level"] for node in original_nodes]
            result_levels = [node["level"] for node in result_nodes]
            
            # Jaccard similarity для уровней
            original_level_set = set(original_levels)
            result_level_set = set(result_levels)
            level_similarity = len(original_level_set & result_level_set) / len(original_level_set | result_level_set)
            
            # Сравниваем заголовки напрямую
            original_titles = set(node["title"].lower() for node in original_nodes)
            result_titles = set(node["title"].lower() for node in result_nodes)
            title_similarity = len(original_titles & result_titles) / len(original_titles | result_titles)
            
            # Сравниваем количество узлов
            count_similarity = 1.0 - abs(len(original_nodes) - len(result_nodes)) / max(len(original_nodes), len(result_nodes))
            
            # Средневзвешенное
            structural_similarity = (
                level_similarity * 0.4 + 
                title_similarity * 0.4 + 
                count_similarity * 0.2
            )
            
            return structural_similarity
            
        except Exception as e:
            self.logger.error(f"Error calculating structural similarity: {e}")
            return 0.0
    
    async def _calculate_semantic_similarity(
        self,
        original_nodes: List[Dict[str, Any]],
        result_nodes: List[Dict[str, Any]]
    ) -> float:
        """Расчет семантического сходства с помощью sentence transformers"""
        try:
            if not self.semantic_model or not original_nodes or not result_nodes:
                return 0.0
            
            # Извлекаем тексты заголовков
            original_texts = [node["title"] for node in original_nodes if node["title"].strip()]
            result_texts = [node["title"] for node in result_nodes if node["title"].strip()]
            
            if not original_texts or not result_texts:
                return 0.0
            
            # Получаем embeddings
            original_embeddings = self.semantic_model.encode(original_texts, convert_to_tensor=True)
            result_embeddings = self.semantic_model.encode(result_texts, convert_to_tensor=True)
            
            # Рассчитываем средние векторы
            original_mean = torch.mean(original_embeddings, dim=0)
            result_mean = torch.mean(result_embeddings, dim=0)
            
            # Cosine similarity
            semantic_similarity = float(util.cos_sim(original_mean, result_mean).item())
            
            return max(0.0, semantic_similarity)
            
        except Exception as e:
            self.logger.error(f"Error calculating semantic similarity: {e}")
            return 0.0
    
    async def _compare_individual_nodes(
        self,
        original_nodes: List[Dict[str, Any]],
        result_nodes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Детальное сравнение отдельных узлов"""
        node_comparisons = []
        
        try:
            # Создаем маппинг по заголовкам для быстрого поиска
            result_nodes_map = {node["title"].lower(): node for node in result_nodes}
            
            for orig_node in original_nodes:
                orig_title_lower = orig_node["title"].lower()
                
                # Ищем точное соответствие
                if orig_title_lower in result_nodes_map:
                    result_node = result_nodes_map[orig_title_lower]
                    similarity = 1.0
                    match_type = "exact"
                else:
                    # Ищем приближенное соответствие
                    best_match = None
                    best_similarity = 0.0
                    
                    for result_node in result_nodes:
                        # Используем Levenshtein distance
                        sim = 1.0 - (levenshtein.distance(orig_title_lower, result_node["title"].lower()) / 
                                    max(len(orig_title_lower), len(result_node["title"].lower())))
                        
                        if sim > best_similarity:
                            best_similarity = sim
                            best_match = result_node
                    
                    if best_match and best_similarity >= self.config.min_node_similarity:
                        result_node = best_match
                        similarity = best_similarity
                        match_type = "approximate"
                    else:
                        result_node = None
                        similarity = 0.0
                        match_type = "missing"
                
                comparison = {
                    "original_title": orig_node["title"],
                    "original_level": orig_node["level"],
                    "result_title": result_node["title"] if result_node else None,
                    "result_level": result_node["level"] if result_node else None,
                    "similarity": similarity,
                    "match_type": match_type,
                    "level_match": (orig_node["level"] == result_node["level"]) if result_node else False
                }
                
                node_comparisons.append(comparison)
            
            return node_comparisons
            
        except Exception as e:
            self.logger.error(f"Error comparing individual nodes: {e}")
            return []
    
    def _analyze_issues(
        self,
        original_nodes: List[Dict[str, Any]],
        result_nodes: List[Dict[str, Any]],
        node_comparisons: List[Dict[str, Any]],
        overall_similarity: float
    ) -> Tuple[List[str], List[str]]:
        """Анализ проблем и генерация рекомендаций"""
        issues_found = []
        recommendations = []
        
        try:
            # Проверяем общее сходство
            if overall_similarity < self.config.similarity_threshold:
                issues_found.append(f"Overall similarity ({overall_similarity:.2f}) below threshold ({self.config.similarity_threshold})")
                recommendations.append("Review document structure and heading consistency")
            
            # Анализируем пропущенные узлы
            missing_nodes = [comp for comp in node_comparisons if comp["match_type"] == "missing"]
            if missing_nodes:
                issues_found.append(f"{len(missing_nodes)} headings not found in result document")
                missing_titles = [node["original_title"] for node in missing_nodes[:5]]  # Показываем первые 5
                issues_found.append(f"Missing headings: {', '.join(missing_titles)}")
                recommendations.append("Check for missing sections in document conversion")
            
            # Анализируем изменения уровней
            level_changes = [comp for comp in node_comparisons if comp["level_match"] == False and comp["match_type"] != "missing"]
            if level_changes:
                issues_found.append(f"{len(level_changes)} headings have different levels")
                recommendations.append("Verify heading hierarchy is preserved correctly")
            
            # Проверяем разницу в количестве узлов
            node_diff = abs(len(original_nodes) - len(result_nodes))
            if node_diff > len(original_nodes) * 0.1:  # Более 10% разницы
                issues_found.append(f"Significant difference in heading count: {node_diff} headings")
                recommendations.append("Review document structure for added or removed sections")
            
            # Анализируем приблизительные совпадения
            approximate_matches = [comp for comp in node_comparisons if comp["match_type"] == "approximate"]
            if len(approximate_matches) > len(original_nodes) * 0.2:  # Более 20% приблизительных совпадений
                issues_found.append(f"{len(approximate_matches)} headings have only approximate matches")
                recommendations.append("Check for translation or formatting inconsistencies in headings")
            
        except Exception as e:
            self.logger.error(f"Error analyzing issues: {e}")
            issues_found.append("Error during issue analysis")
        
        return issues_found, recommendations

# =======================================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =======================================================================================

def create_ast_comparator(config: Optional[ASTComparisonConfig] = None) -> ASTComparator:
    """Фабричная функция для создания AST Comparator"""
    return ASTComparator(config)

async def compare_document_structures(
    original_ast: Dict[str, Any],
    result_ast: Dict[str, Any],
    comparison_id: str,
    config: Optional[ASTComparisonConfig] = None
) -> ASTComparisonResult:
    """
    Высокоуровневая функция для сравнения структур документов
    
    Args:
        original_ast: AST оригинального документа
        result_ast: AST результирующего документа
        comparison_id: Идентификатор сравнения
        config: Конфигурация компаратора
        
    Returns:
        ASTComparisonResult: Результат сравнения
    """
    comparator = create_ast_comparator(config)
    return await comparator.compare_ast_structures(original_ast, result_ast, comparison_id)

# =======================================================================================
# ОСНОВНОЙ БЛОК ДЛЯ ТЕСТИРОВАНИЯ
# =======================================================================================

if __name__ == "__main__":
    # Пример использования
    async def main():
        config = ASTComparisonConfig()
        comparator = ASTComparator(config)
        
        # Тестовые AST структуры
        original_ast = {
            "title": "root",
            "level": 0,
            "children": [
                {
                    "title": "Introduction",
                    "level": 1,
                    "children": [
                        {"title": "Overview", "level": 2, "children": []},
                        {"title": "Scope", "level": 2, "children": []}
                    ]
                },
                {
                    "title": "Methodology", 
                    "level": 1,
                    "children": [
                        {"title": "Data Collection", "level": 2, "children": []}
                    ]
                }
            ]
        }
        
        result_ast = {
            "title": "root",
            "level": 0,
            "children": [
                {
                    "title": "介绍",  # "Introduction" в переводе
                    "level": 1,
                    "children": [
                        {"title": "概述", "level": 2, "children": []},  # "Overview"
                        {"title": "范围", "level": 2, "children": []}   # "Scope"
                    ]
                },
                {
                    "title": "方法论",  # "Methodology"
                    "level": 1,
                    "children": [
                        {"title": "数据收集", "level": 2, "children": []}  # "Data Collection"
                    ]
                }
            ]
        }
        
        result = await comparator.compare_ast_structures(original_ast, result_ast, "test_comparison")
        
        print(f"Overall similarity: {result.overall_similarity:.2f}")
        print(f"Structural similarity: {result.structural_similarity:.2f}")
        print(f"Semantic similarity: {result.semantic_similarity:.2f}")
        print(f"Issues found: {len(result.issues_found)}")
        
        if result.issues_found:
            print("Issues:")
            for issue in result.issues_found:
                print(f"  - {issue}")
    
    asyncio.run(main())