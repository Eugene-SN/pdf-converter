#!/bin/bash

# ===============================================================================
# PDF CONVERTER PIPELINE v2.0 - УНИВЕРСАЛЬНЫЙ ПЕРЕВОДЧИК ДОКУМЕНТОВ
# Скрипт для перевода документов с выбором различных сценариев работы
# Поддерживает: конвертация+перевод, перевод готовых MD файлов
# ===============================================================================

set -euo pipefail

# Конфигурация
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/.env"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# Загрузка конфигурации
if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
fi

# Конфигурация сервисов
AIRFLOW_URL="${AIRFLOW_BASE_URL_HOST:-http://localhost:8090}"
AIRFLOW_USERNAME="${AIRFLOW_USERNAME:-admin}"
AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-admin}"

# Локальные папки
HOST_INPUT_DIR="${SCRIPT_DIR}/input"
HOST_OUTPUT_ZH_DIR="${SCRIPT_DIR}/output/zh"
HOST_OUTPUT_RU_DIR="${SCRIPT_DIR}/output/ru"
HOST_OUTPUT_EN_DIR="${SCRIPT_DIR}/output/en"
LOGS_DIR="${SCRIPT_DIR}/logs"

# Создание директорий
mkdir -p "$HOST_INPUT_DIR" "$HOST_OUTPUT_ZH_DIR" "$HOST_OUTPUT_RU_DIR" "$HOST_OUTPUT_EN_DIR" "$LOGS_DIR"

# Настройки логирования для одного запуска
LOG_FILE="${LOGS_DIR}/translation_$(date +%Y%m%d_%H%M%S)_$$.log"
touch "$LOG_FILE"

log_file_path() {
    echo "$LOG_FILE"
}

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${BLUE}[$timestamp]${NC} ${YELLOW}[$level]${NC} $message" | tee -a "$(log_file_path)"
}

show_header() {
    echo -e "${CYAN}"
    echo "==============================================================================="
    echo " PDF CONVERTER PIPELINE v2.0 - УНИВЕРСАЛЬНЫЙ ПЕРЕВОДЧИК"
    echo "==============================================================================="
    echo -e "${NC}"
    echo "🌐 Многоязычный переводчик документов"
    echo "📂 Входные PDF: $HOST_INPUT_DIR"
    echo "📂 Исходные MD: $HOST_OUTPUT_ZH_DIR"
    echo "📁 Русский: $HOST_OUTPUT_RU_DIR"
    echo "📁 Английский: $HOST_OUTPUT_EN_DIR"
    echo ""
}

show_menu() {
    echo -e "${MAGENTA}=== ВЫБЕРИТЕ СЦЕНАРИЙ РАБОТЫ ===${NC}"
    echo ""
    echo -e "${GREEN}1.${NC} ${BLUE}Полная обработка PDF → Русский${NC}"
    echo "   (Конвертация PDF в MD + перевод на русский)"
    echo ""
    echo -e "${GREEN}2.${NC} ${BLUE}Полная обработка PDF → Английский${NC}"
    echo "   (Конвертация PDF в MD + перевод на английский)"
    echo ""
    echo -e "${GREEN}3.${NC} ${BLUE}Перевод готового MD → Русский${NC}"
    echo "   (Перевод существующих MD файлов из $HOST_OUTPUT_ZH_DIR)"
    echo ""
    echo -e "${GREEN}4.${NC} ${BLUE}Перевод готового MD → Английский${NC}"
    echo "   (Перевод существующих MD файлов из $HOST_OUTPUT_ZH_DIR)"
    echo ""
    echo -e "${GREEN}5.${NC} ${BLUE}Только конвертация PDF → MD${NC}"
    echo "   (Конвертация без перевода, сохранение в $HOST_OUTPUT_ZH_DIR)"
    echo ""
    echo -e "${RED}0.${NC} ${YELLOW}Выход${NC}"
    echo ""
    echo -n -e "${CYAN}Введите номер (0-5): ${NC}"
}

check_services() {
    log "INFO" "Проверка готовности сервисов..."
    local services=(
        "$AIRFLOW_URL/health:Airflow UI"
    )

    for service_info in "${services[@]}"; do
        local url="${service_info%:*}"
        local name="${service_info#*:}"
        if curl -s --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" "$url" > /dev/null 2>&1; then
            log "INFO" "✅ $name готов"
        else
            local base="${url%/health}"
            local v2="${base}/api/v2/monitor/health"
            if ! curl -s --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" "$v2" > /dev/null 2>&1; then
                log "ERROR" "$name недоступен на $url"
                return 1
            fi
            log "INFO" "✅ $name готов (через API v2)"
        fi
    done

    log "INFO" "✅ Все сервисы готовы"
    return 0
}

# Сценарий 1 и 2: Полная обработка PDF с переводом
full_pdf_processing() {
    local target_language="$1"
    local lang_name="$2"
    local output_dir="$3"

    log "INFO" "🚀 Запуск полной обработки PDF → $lang_name"

    # Поиск PDF файлов
    local pdf_files=()
    while IFS= read -r -d '' file; do
        pdf_files+=("$file")
    done < <(find "$HOST_INPUT_DIR" -name "*.pdf" -type f -print0)

    local total_files=${#pdf_files[@]}

    if [ $total_files -eq 0 ]; then
        log "WARN" "📂 Нет PDF файлов в $HOST_INPUT_DIR"
        echo "Поместите PDF файлы в папку $HOST_INPUT_DIR и запустите снова"
        return 0
    fi

    log "INFO" "📊 Найдено файлов для обработки: $total_files"
    echo ""

    # Обработка каждого файла
    local processed=0
    local failed=0
    local start_time
    start_time=$(date +%s)

    for pdf_file in "${pdf_files[@]}"; do
        local filename
        filename=$(basename "$pdf_file")
        echo -e "${BLUE}[ФАЙЛ $((processed + failed + 1))/$total_files]${NC} $filename → $lang_name"

        if process_pdf_with_translation "$pdf_file" "$target_language" "$output_dir"; then
            ((processed++))
            echo -e "Статус: ${GREEN}✅ УСПЕШНО ПЕРЕВЕДЕН${NC}"
        else
            ((failed++))
            echo -e "Статус: ${RED}❌ ОШИБКА ОБРАБОТКИ${NC}"
        fi
        echo ""
    done

    # Итоговая статистика
    local end_time
    end_time=$(date +%s)
    local total_duration=$((end_time - start_time))

    show_processing_results "$processed" "$failed" "$total_duration" "$lang_name" "$output_dir"
}

# Обработка одного PDF файла с переводом
process_pdf_with_translation() {
    local pdf_file="$1"
    local target_language="$2"
    local output_dir="$3"
    local filename
    filename=$(basename "$pdf_file")
    local timestamp
    timestamp=$(date +%s)

    # Конфигурация для полной обработки с переводом через orchestrator
    local config="{
        "input_file": "$pdf_file",
        "filename": "$filename",
        "timestamp": $timestamp,
        "target_language": "$target_language",
        "quality_level": "high",
        "enable_ocr": true,
        "preserve_structure": true,
        "extract_tables": true,
        "extract_images": true,
        "stage_mode": "full_with_translation",
        "processing_stages": 4,
        "validation_enabled": true,
        "quality_target": 95.0,
        "language": "zh-CN",
        "chinese_optimization": true,
        "pipeline_version": "4.0",
        "processing_mode": "digital_pdf",
        "use_orchestrator": true,
        "preserve_technical_terms": true
    }"

    # Запуск через orchestrator
    local response
    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "{"conf": $config}" \
        "$AIRFLOW_URL/api/v1/dags/orchestrator_dag/dagRuns")

    local http_code
    http_code=$(echo "$response" | tail -n1)
    local body
    body=$(echo "$response" | head -n -1)

    if [ "$http_code" -eq 200 ] || [ "$http_code" -eq 201 ]; then
        local dag_run_id
        dag_run_id=$(echo "$body" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('dag_run_id', 'unknown'))" 2>/dev/null || echo "unknown")
        log "INFO" "✅ Обработка запущена. Run ID: $dag_run_id"

        # Ожидание завершения
        wait_for_translation_completion "$dag_run_id" "$filename" "$target_language"
        return $?
    else
        log "ERROR" "❌ Ошибка запуска обработки: HTTP $http_code"
        return 1
    fi
}

# Сценарий 3 и 4: Перевод готовых MD файлов
translate_existing_md() {
    local target_language="$1"
    local lang_name="$2"
    local output_dir="$3"

    log "INFO" "🔄 Запуск перевода готовых MD файлов → $lang_name"

    # Поиск MD файлов
    local md_files=()
    while IFS= read -r -d '' file; do
        md_files+=("$file")
    done < <(find "$HOST_OUTPUT_ZH_DIR" -name "*.md" -type f -print0)

    local total_files=${#md_files[@]}

    if [ $total_files -eq 0 ]; then
        log "WARN" "📂 Нет MD файлов в $HOST_OUTPUT_ZH_DIR"
        echo "Сначала выполните конвертацию PDF или поместите MD файлы в $HOST_OUTPUT_ZH_DIR"
        return 0
    fi

    log "INFO" "📊 Найдено файлов для перевода: $total_files"
    echo ""

    # Обработка каждого файла
    local processed=0
    local failed=0
    local start_time
    start_time=$(date +%s)

    for md_file in "${md_files[@]}"; do
        local filename
        filename=$(basename "$md_file")
        echo -e "${BLUE}[ФАЙЛ $((processed + failed + 1))/$total_files]${NC} $filename → $lang_name"

        if translate_single_md "$md_file" "$target_language" "$output_dir"; then
            ((processed++))
            echo -e "Статус: ${GREEN}✅ УСПЕШНО ПЕРЕВЕДЕН${NC}"
        else
            ((failed++))
            echo -e "Статус: ${RED}❌ ОШИБКА ПЕРЕВОДА${NC}"
        fi
        echo ""
    done

    # Итоговая статистика
    local end_time
    end_time=$(date +%s)
    local total_duration=$((end_time - start_time))

    show_processing_results "$processed" "$failed" "$total_duration" "$lang_name" "$output_dir"
}

# Перевод одного MD файла
translate_single_md() {
    local md_file="$1"
    local target_language="$2"
    local output_dir="$3"
    local filename
    filename=$(basename "$md_file")
    local timestamp
    timestamp=$(date +%s)

    # Конфигурация для перевода готового MD
    local config="{
        "markdown_file": "$md_file",
        "filename": "$filename",
        "timestamp": $timestamp,
        "target_language": "$target_language",
        "stage_mode": "translation_only",
        "preserve_technical_terms": true,
        "chinese_source": true,
        "translation_method": "builtin_dictionary_v3",
        "use_orchestrator": false,
        "stage3_only": true
    }"

    # Запуск translation_pipeline напрямую
    local response
    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "{"conf": $config}" \
        "$AIRFLOW_URL/api/v1/dags/translation_pipeline/dagRuns")

    local http_code
    http_code=$(echo "$response" | tail -n1)
    local body
    body=$(echo "$response" | head -n -1)

    if [ "$http_code" -eq 200 ] || [ "$http_code" -eq 201 ]; then
        local dag_run_id
        dag_run_id=$(echo "$body" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('dag_run_id', 'unknown'))" 2>/dev/null || echo "unknown")
        log "INFO" "✅ Перевод запущен. Run ID: $dag_run_id"

        # Ожидание завершения
        wait_for_translation_completion "$dag_run_id" "$filename" "$target_language"
        return $?
    else
        log "ERROR" "❌ Ошибка запуска перевода: HTTP $http_code"
        return 1
    fi
}

# Сценарий 5: Только конвертация
convert_only() {
    log "INFO" "🔄 Запуск конвертации PDF → MD (без перевода)"

    # Используем основной скрипт конвертации
    if [ -f "$SCRIPT_DIR/convert-pdf-to-markdown.sh" ]; then
        bash "$SCRIPT_DIR/convert-pdf-to-markdown.sh"
    else
        log "ERROR" "❌ Скрипт convert-pdf-to-markdown.sh не найден"
        echo "Создайте скрипт конвертации или запустите полную обработку"
        return 1
    fi
}

wait_for_translation_completion() {
    local dag_run_id="$1"
    local filename="$2"
    local target_language="$3"
    local timeout=1800  # 30 минут
    local start_time
    start_time=$(date +%s)

    log "INFO" "⏳ Ожидание завершения обработки (таймаут: ${timeout}s)..."

    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -gt $timeout ]; then
            log "ERROR" "❌ Таймаут обработки"
            return 1
        fi

        # Получение статуса DAG (определяем какой DAG проверять)
        local dag_name="orchestrator_dag"
        if [[ "$dag_run_id" == *"translation_pipeline"* ]]; then
            dag_name="translation_pipeline"
        fi

        local response
        response=$(curl -s \
            --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
            "$AIRFLOW_URL/api/v1/dags/$dag_name/dagRuns/$dag_run_id")

        local state
        state=$(echo "$response" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('state', 'unknown'))
except:
    print('error')
" 2>/dev/null || echo "error")

        case "$state" in
            "success")
                log "INFO" "✅ Обработка завершена успешно!"
                return 0
                ;;
            "failed"|"upstream_failed")
                log "ERROR" "❌ Обработка завершена с ошибкой"
                return 1
                ;;
            "running")
                local progress_msg="Выполняется (${elapsed}s)"
                printf "\r${YELLOW}[ОБРАБОТКА]${NC} $progress_msg "
                sleep 10
                ;;
            *)
                sleep 5
                ;;
        esac
    done
}

show_processing_results() {
    local processed="$1"
    local failed="$2"
    local duration="$3"
    local lang_name="$4"
    local output_dir="$5"

    echo "==============================================================================="
    echo -e "${GREEN}ОБРАБОТКА ЗАВЕРШЕНА → $lang_name${NC}"
    echo "==============================================================================="
    echo -e "📊 Статистика:"
    echo -e " Успешно обработано: ${GREEN}$processed${NC} файлов"
    echo -e " Ошибок: ${RED}$failed${NC} файлов"
    echo -e " Общее время: ${BLUE}$duration${NC} секунд"
    echo ""
    echo -e "📁 Результаты сохранены в: ${YELLOW}$output_dir${NC}"
    echo -e "📋 Логи сохранены в: ${YELLOW}$LOGS_DIR${NC}"
    echo ""

    if [ $failed -gt 0 ]; then
        echo -e "${YELLOW}⚠️ Рекомендации по устранению ошибок:${NC}"
        echo " - Проверьте логи для диагностики"
        echo " - Убедитесь в корректности исходных файлов"
        echo " - Проверьте работу всех сервисов"
    else
        echo -e "${GREEN}🎉 Все файлы успешно обработаны!${NC}"
    fi
}

# Основная логика
main() {
    show_header
    log "INFO" "Запись лога: $(log_file_path)"

    if ! check_services; then
        echo -e "${RED}❌ Сервисы недоступны. Запустите: docker-compose up -d${NC}"
        exit 1
    fi

    while true; do
        echo ""
        show_menu
        read -r choice

        case $choice in
            1)
                echo -e "${GREEN}Выбран сценарий: Полная обработка PDF → Русский${NC}"
                full_pdf_processing "ru" "Русский" "$HOST_OUTPUT_RU_DIR"
                ;;
            2)
                echo -e "${GREEN}Выбран сценарий: Полная обработка PDF → Английский${NC}"
                full_pdf_processing "en" "Английский" "$HOST_OUTPUT_EN_DIR"
                ;;
            3)
                echo -e "${GREEN}Выбран сценарий: Перевод MD → Русский${NC}"
                translate_existing_md "ru" "Русский" "$HOST_OUTPUT_RU_DIR"
                ;;
            4)
                echo -e "${GREEN}Выбран сценарий: Перевод MD → Английский${NC}"
                translate_existing_md "en" "Английский" "$HOST_OUTPUT_EN_DIR"
                ;;
            5)
                echo -e "${GREEN}Выбран сценарий: Только конвертация PDF → MD${NC}"
                convert_only
                ;;
            0)
                echo -e "${YELLOW}Выход из программы${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}❌ Некорректный выбор. Попробуйте снова.${NC}"
                ;;
        esac

        echo ""
        echo -e "${CYAN}Нажмите Enter для возврата в главное меню...${NC}"
        read -r
    done
}

# Запуск, если скрипт вызван напрямую
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
