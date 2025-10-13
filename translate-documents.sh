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

# Настройки логирования
LOG_FILE=""

start_new_log() {
    local prefix="$1"
    LOG_FILE="${LOGS_DIR}/${prefix}_$(date +%Y%m%d_%H%M%S)_$$.log"
    touch "$LOG_FILE"
}

log_file_path() {
    if [ -z "$LOG_FILE" ]; then
        start_new_log "session"
    fi
    echo "$LOG_FILE"
}

prepare_logging() {
    local prefix="$1"
    start_new_log "$prefix"
    log "INFO" "Запись лога: $(log_file_path)"
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

format_duration() {
    local total=${1:-0}

    if ! [[ "$total" =~ ^[0-9]+$ ]]; then
        total=0
    fi

    local hours=$((total / 3600))
    local minutes=$(((total % 3600) / 60))
    local seconds=$((total % 60))

    if [ $hours -gt 0 ]; then
        printf "%02d:%02d:%02d" "$hours" "$minutes" "$seconds"
    else
        printf "%02d:%02d" "$minutes" "$seconds"
    fi
}

render_progress_bar() {
    local current=${1:-0}
    local total=${2:-1}
    local width=${3:-30}

    if [ "$total" -le 0 ]; then
        total=1
    fi

    if [ "$current" -lt 0 ]; then
        current=0
    fi

    if [ "$current" -gt "$total" ]; then
        current=$total
    fi

    local percent=$((current * 100 / total))
    local filled=$((percent * width / 100))
    local empty=$((width - filled))
    local bar=""

    for ((i = 0; i < filled; i++)); do
        bar+="█"
    done

    for ((i = 0; i < empty; i++)); do
        bar+="·"
    done

    printf "[%s] %3d%%" "$bar" "$percent"
}

fetch_task_overview() {
    local dag_name="$1"
    local dag_run_id="$2"

    local response
    response=$(curl -s \
        --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
        "$AIRFLOW_URL/api/v1/dags/$dag_name/dagRuns/$dag_run_id/taskInstances")

    if [ -z "$response" ]; then
        echo "error"
        return 1
    fi

    local overview
    overview=$(echo "$response" | jq -r '{
        total: (.task_instances | length),
        success: ([.task_instances[] | select(.state == "success")] | length),
        running: ([.task_instances[] | select(.state == "running")] | length),
        failed: ([.task_instances[] | select(.state == "failed" or .state == "upstream_failed")] | length),
        queued: ([.task_instances[] | select(.state == "queued" or .state == "scheduled")] | length),
        retries: ([.task_instances[] | select(.state == "up_for_retry")] | length),
        chunks_total: ([.task_instances[] | select((.task_id // "") | test("chunk"; "i"))] | length),
        chunks_done: ([.task_instances[] | select((.task_id // "") | test("chunk"; "i") and .state == "success")] | length)
    } | "\(.total)|\(.success)|\(.running)|\(.failed)|\(.queued)|\(.retries)|\(.chunks_total)|\(.chunks_done)"' 2>/dev/null)

    if [ -z "$overview" ] || [ "$overview" == "null" ]; then
        echo "error"
        return 1
    fi

    echo "$overview"
}

show_header() {
    echo -e "${CYAN}"
    echo "==============================================================================="
    echo " PDF CONVERTER PIPELINE v2.0 - ЕДИНЫЙ КОНВЕРТЕР И ПЕРЕВОДЧИК"
    echo "==============================================================================="
    echo -e "${NC}"
    echo "🌐 Многоязычный переводчик и конвертер PDF"
    echo "🧭 Режимы: конвертация с 5-уровневой валидацией и перевод готовых Markdown"
    echo "📂 Входные PDF: $HOST_INPUT_DIR"
    echo "📂 Исходные MD (ZH): $HOST_OUTPUT_ZH_DIR"
    echo "📁 Русский перевод: $HOST_OUTPUT_RU_DIR"
    echo "📁 Английский перевод: $HOST_OUTPUT_EN_DIR"
    echo "📋 Логи: $LOGS_DIR"
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
    echo "   (Полная конвертация с 5-уровневой валидацией, без перевода, выход в $HOST_OUTPUT_ZH_DIR)"
    echo ""
    echo -e "${RED}0.${NC} ${YELLOW}Выход${NC}"
    echo ""
    echo -n -e "${CYAN}Введите номер (0-5): ${NC}"
}

check_dependencies() {
    log "INFO" "🔧 Проверка зависимостей..."

    if ! command -v jq &> /dev/null; then
        log "ERROR" "❌ jq не установлен. Установите: sudo apt-get install jq"
        return 1
    fi
    log "INFO" "✅ jq установлен"

    if ! command -v curl &> /dev/null; then
        log "ERROR" "❌ curl не установлен. Установите: sudo apt-get install curl"
        return 1
    fi
    log "INFO" "✅ curl установлен"

    return 0
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

    # Конфигурация для полной обработки с переводом через orchestrator (через jq)
    local config_json
    config_json=$(jq -n \
        --arg input_file "$pdf_file" \
        --arg filename "$filename" \
        --argjson timestamp $timestamp \
        --arg target_language "$target_language" \
        --arg quality_level "high" \
        --argjson enable_ocr true \
        --argjson preserve_structure true \
        --argjson extract_tables true \
        --argjson extract_images true \
        --arg stage_mode "full_with_translation" \
        --argjson processing_stages 4 \
        --argjson validation_enabled true \
        --argjson quality_target 95.0 \
        --arg language "zh-CN" \
        --argjson chinese_optimization true \
        --arg pipeline_version "4.0" \
        --arg processing_mode "digital_pdf" \
        --argjson use_orchestrator true \
        --argjson preserve_technical_terms true \
        '{
            input_file: $input_file,
            filename: $filename,
            timestamp: $timestamp,
            target_language: $target_language,
            quality_level: $quality_level,
            enable_ocr: $enable_ocr,
            preserve_structure: $preserve_structure,
            extract_tables: $extract_tables,
            extract_images: $extract_images,
            stage_mode: $stage_mode,
            processing_stages: $processing_stages,
            validation_enabled: $validation_enabled,
            quality_target: $quality_target,
            language: $language,
            chinese_optimization: $chinese_optimization,
            pipeline_version: $pipeline_version,
            processing_mode: $processing_mode,
            use_orchestrator: $use_orchestrator,
            preserve_technical_terms: $preserve_technical_terms
        }')

    local request_body
    request_body=$(jq -n --argjson conf "$config_json" '{conf: $conf}')

    # Запуск через orchestrator
    local response
    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "$request_body" \
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
        if wait_for_translation_completion "$dag_run_id" "$filename" "$target_language"; then
            show_translation_results "$filename" "$target_language" "$output_dir"
            return 0
        fi
        return 1
    else
        if [ -n "$body" ]; then
            log "ERROR" "❌ Ошибка запуска обработки: HTTP $http_code — $(echo "$body" | tr '\n' ' ')"
        else
            log "ERROR" "❌ Ошибка запуска обработки: HTTP $http_code"
        fi
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
    local config_json
    config_json=$(jq -n \
        --arg markdown_file "$md_file" \
        --arg filename "$filename" \
        --argjson timestamp $timestamp \
        --arg target_language "$target_language" \
        --arg stage_mode "translation_only" \
        --argjson preserve_technical_terms true \
        --argjson chinese_source true \
        --arg translation_method "builtin_dictionary_v3" \
        --argjson use_orchestrator false \
        --argjson stage3_only true \
        '{
            markdown_file: $markdown_file,
            filename: $filename,
            timestamp: $timestamp,
            target_language: $target_language,
            stage_mode: $stage_mode,
            preserve_technical_terms: $preserve_technical_terms,
            chinese_source: $chinese_source,
            translation_method: $translation_method,
            use_orchestrator: $use_orchestrator,
            stage3_only: $stage3_only
        }')

    local request_body
    request_body=$(jq -n --argjson conf "$config_json" '{conf: $conf}')

    # Запуск translation_pipeline напрямую
    local response
    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "$request_body" \
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
        if wait_for_translation_completion "$dag_run_id" "$filename" "$target_language"; then
            show_translation_results "$filename" "$target_language" "$output_dir"
            return 0
        fi
        return 1
    else
        if [ -n "$body" ]; then
            log "ERROR" "❌ Ошибка запуска перевода: HTTP $http_code — $(echo "$body" | tr '\n' ' ')"
        else
            log "ERROR" "❌ Ошибка запуска перевода: HTTP $http_code"
        fi
        return 1
    fi
}

find_related_output_file() {
    local output_dir="$1"
    local source_filename="$2"

    local base
    base="${source_filename%.*}"

    local latest_match=""
    local latest_ts=0

    while IFS= read -r -d '' file; do
        local ts
        if ts=$(stat -c %Y "$file" 2>/dev/null); then
            :
        else
            ts=$(stat -f %m "$file" 2>/dev/null || echo 0)
        fi

        if [[ "$(basename "$file")" == "$base"* ]] && [ "$ts" -ge "$latest_ts" ]; then
            latest_match="$file"
            latest_ts=$ts
        fi
    done < <(find "$output_dir" -maxdepth 1 -type f \( -name "*.md" -o -name "*.txt" \) -print0 2>/dev/null)

    if [ -n "$latest_match" ]; then
        echo "$latest_match"
        return 0
    fi

    local fallback_file=""
    local fallback_ts=0

    while IFS= read -r -d '' file; do
        local ts
        if ts=$(stat -c %Y "$file" 2>/dev/null); then
            :
        else
            ts=$(stat -f %m "$file" 2>/dev/null || echo 0)
        fi

        if [ "$ts" -ge "$fallback_ts" ]; then
            fallback_ts=$ts
            fallback_file="$file"
        fi
    done < <(find "$output_dir" -maxdepth 1 -type f \( -name "*.md" -o -name "*.txt" \) -print0 2>/dev/null)

    echo "$fallback_file"
}

analyze_markdown_file() {
    local file="$1"
    local context="$2"

    if [ -z "$file" ] || [ ! -f "$file" ]; then
        log "WARN" "⚠️ Файл для анализа не найден ($context)"
        return 1
    fi

    local lines words chars
    lines=$(wc -l < "$file" 2>/dev/null || echo 0)
    words=$(wc -w < "$file" 2>/dev/null || echo 0)
    chars=$(wc -m < "$file" 2>/dev/null || wc -c < "$file" 2>/dev/null || echo 0)

    local tables
    tables=$(awk '
        BEGIN { tables = 0; in_table = 0 }
        /^[[:space:]]*\|.*\|[[:space:]]*$/ {
            if (in_table == 0) {
                tables++
                in_table = 1
            }
            next
        }
        /^[[:space:]]*:-{2,}/ {
            if (in_table == 0) {
                tables++
                in_table = 1
            }
            next
        }
        {
            in_table = 0
        }
        END { print tables }
    ' "$file" 2>/dev/null || echo 0)

    local images links headers lists code_blocks chunk_markers paragraphs
    images=$(grep -cE '![^!]*\[[^]]*\]\([^)]*\)' "$file" 2>/dev/null || echo 0)
    links=$(grep -cE '\[[^]]+\]\([^)]*\)' "$file" 2>/dev/null || echo 0)
    headers=$(grep -cE '^[[:space:]]*#{1,6}[[:space:]]+' "$file" 2>/dev/null || echo 0)
    lists=$(grep -cE '^[[:space:]]*([-*+]\s+|[0-9]+\.\s+)' "$file" 2>/dev/null || echo 0)
    code_blocks=$(grep -cE '^[[:space:]]*```' "$file" 2>/dev/null || echo 0)
    chunk_markers=$(grep -ciE '<!--\s*chunk' "$file" 2>/dev/null || echo 0)

    if [ "$chunk_markers" -eq 0 ]; then
        paragraphs=$(awk '
            BEGIN { chunks = 0; in_para = 0 }
            /[^[:space:]]/ {
                if (in_para == 0) {
                    chunks++
                    in_para = 1
                }
                next
            }
            {
                in_para = 0
            }
            END { print chunks }
        ' "$file" 2>/dev/null || echo 0)
        chunk_markers=$paragraphs
    fi

    log "INFO" "📦 Аналитика результата ($context):"
    log "INFO" "  • Строк: $lines"
    log "INFO" "  • Слов: $words"
    log "INFO" "  • Символов: $chars"
    log "INFO" "  • Заголовков: $headers"
    log "INFO" "  • Таблиц: $tables"
    log "INFO" "  • Изображений: $images"
    log "INFO" "  • Списков: $lists"
    log "INFO" "  • Блоков кода: $code_blocks"
    log "INFO" "  • Ссылок: $links"
    log "INFO" "  • Чанков: $chunk_markers"

    local chars_numeric="$chars"
    if ! [[ "$chars_numeric" =~ ^[0-9]+$ ]]; then
        chars_numeric=0
    fi

    if [ "$chars_numeric" -ge 100 ]; then
        log "INFO" "✅ Качество: файл содержит достаточно контента"
    else
        log "WARN" "⚠️ Качество: файл может быть слишком коротким"
    fi

    log "INFO" "📄 Превью содержимого:"
    head -5 "$file" | sed 's/^/  /'
}

show_translation_results() {
    local filename="$1"
    local target_language="$2"
    local output_dir="$3"

    local result_file
    result_file=$(find_related_output_file "$output_dir" "$filename")

    if [ -z "$result_file" ]; then
        log "WARN" "⚠️ Не удалось определить результат перевода для $filename"
        return 1
    fi

    log "INFO" "📁 Результирующий файл ($target_language): $result_file"
    analyze_markdown_file "$result_file" "Перевод → $target_language"
}

# Сценарий 5: Только конвертация

trigger_full_conversion() {
    local pdf_file="$1"
    local filename
    filename=$(basename "$pdf_file")
    local timestamp
    timestamp=$(date +%s)

    log "INFO" "🚀 Запуск полной конвертации: $filename"

    local config_json
    config_json=$(jq -n \
        --arg input_file "$pdf_file" \
        --arg filename "$filename" \
        --argjson timestamp $timestamp \
        --arg target_language "original" \
        --arg quality_level "high" \
        --argjson enable_ocr true \
        --argjson preserve_structure true \
        --argjson extract_tables true \
        --argjson extract_images true \
        --arg stage_mode "full_conversion_with_validation" \
        --argjson processing_stages 4 \
        --argjson validation_enabled true \
        --argjson quality_target 100.0 \
        --arg language "zh-CN" \
        --argjson chinese_optimization true \
        --arg pipeline_version "4.0" \
        --arg processing_mode "digital_pdf" \
        --argjson use_orchestrator true \
        '{
            input_file: $input_file,
            filename: $filename,
            timestamp: $timestamp,
            target_language: $target_language,
            quality_level: $quality_level,
            enable_ocr: $enable_ocr,
            preserve_structure: $preserve_structure,
            extract_tables: $extract_tables,
            extract_images: $extract_images,
            stage_mode: $stage_mode,
            processing_stages: $processing_stages,
            validation_enabled: $validation_enabled,
            quality_target: $quality_target,
            language: $language,
            chinese_optimization: $chinese_optimization,
            pipeline_version: $pipeline_version,
            processing_mode: $processing_mode,
            use_orchestrator: $use_orchestrator
        }')

    local request_body
    request_body=$(jq -n --argjson conf "$config_json" '{conf: $conf}')

    log "INFO" "📤 Отправка запроса в Airflow..."

    local response
    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "$request_body" \
        "$AIRFLOW_URL/api/v1/dags/orchestrator_dag/dagRuns")

    local http_code
    http_code=$(echo "$response" | tail -n1)
    local body
    body=$(echo "$response" | head -n -1)

    if [ "$http_code" -eq 200 ] || [ "$http_code" -eq 201 ]; then
        local dag_run_id
        dag_run_id=$(echo "$body" | jq -r '.dag_run_id // "unknown"' 2>/dev/null || echo "unknown")
        log "INFO" "✅ Конвертация запущена. Run ID: $dag_run_id"

        wait_for_conversion_completion "$dag_run_id" "$filename"
        return 0
    else
        log "ERROR" "❌ Ошибка запуска конвертации: HTTP $http_code"
        if [ -n "$body" ]; then
            log "ERROR" "Ответ: $body"
        fi

        if [[ "$body" == *"not valid JSON"* ]]; then
            log "ERROR" "🔧 Проблема с JSON форматированием. Проверьте установку jq"
        elif [[ "$body" == *"orchestrator_dag"* ]]; then
            log "ERROR" "🔧 orchestrator_dag недоступен. Проверьте DAG в Airflow UI"
        fi

        return 1
    fi
}

wait_for_conversion_completion() {
    local dag_run_id="$1"
    local filename="$2"
    local timeout=3600
    local start_time
    start_time=$(date +%s)

    log "INFO" "⏳ Ожидание завершения полной конвертации (таймаут: ${timeout}s)..."

    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -gt $timeout ]; then
            printf "\r\033[K"
            log "ERROR" "❌ Таймаут конвертации"
            return 1
        fi

        local response
        response=$(curl -s \
            --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
            "$AIRFLOW_URL/api/v1/dags/orchestrator_dag/dagRuns/$dag_run_id")

        local state
        state=$(echo "$response" | jq -r '.state // "unknown"' 2>/dev/null || echo "error")

        local overview_data
        overview_data=$(fetch_task_overview "orchestrator_dag" "$dag_run_id")
        local progress_line=""

        if [ -n "$overview_data" ] && [ "$overview_data" != "error" ]; then
            IFS='|' read -r total success_count running_count failed_count queued_count retries_count chunks_total chunks_done <<< "$overview_data"

            local total_count=${total:-0}
            if ! [[ "$total_count" =~ ^[0-9]+$ ]]; then
                total_count=0
            fi

            if [ "$total_count" -le 0 ]; then
                total_count=1
            fi

            local success_numeric=${success_count:-0}
            local failed_numeric=${failed_count:-0}
            local running_numeric=${running_count:-0}
            local queued_numeric=${queued_count:-0}
            local retries_numeric=${retries_count:-0}
            local chunks_total_numeric=${chunks_total:-0}
            local chunks_done_numeric=${chunks_done:-0}

            local value
            for value_name in success_numeric failed_numeric running_numeric queued_numeric retries_numeric chunks_total_numeric chunks_done_numeric; do
                value=${!value_name}
                if ! [[ "$value" =~ ^[0-9]+$ ]]; then
                    printf -v "$value_name" '%d' 0
                fi
            done

            local completed=$(( success_numeric + failed_numeric ))
            local duration_text
            duration_text=$(format_duration "$elapsed")
            local bar
            bar=$(render_progress_bar "$completed" "$total_count" 32)

            progress_line="$bar $duration_text | ✔ $success_numeric • ⏳ $running_numeric • ❌ $failed_numeric • ⏱ $queued_numeric"

            if [ "$retries_numeric" -gt 0 ]; then
                progress_line+=" • 🔁 $retries_numeric"
            fi

            if [ "$chunks_total_numeric" -gt 0 ]; then
                progress_line+=" | чанки: $chunks_done_numeric/$chunks_total_numeric"
            fi
        else
            progress_line="Выполняется ($(format_duration "$elapsed"))"
        fi

        case "$state" in
            "success")
                printf "\r\033[K"
                log "INFO" "✅ Конвертация завершена успешно!"
                show_conversion_results "$filename"
                return 0
                ;;
            "failed"|"upstream_failed")
                printf "\r\033[K"
                log "ERROR" "❌ Конвертация завершена с ошибкой"
                log "ERROR" "🔍 Проверьте детали в Airflow UI: $AIRFLOW_URL/dags/orchestrator_dag/grid?dag_run_id=$dag_run_id"
                return 1
                ;;
            "running"|"queued"|"scheduled")
                printf "\r${YELLOW}[КОНВЕРТАЦИЯ]${NC} %s" "$progress_line"
                sleep 10
                ;;
            *)
                sleep 5
                ;;
        esac
    done
}

show_conversion_results() {
    local filename="$1"

    log "INFO" "📊 Результаты конвертации:"

    local latest_file
    latest_file=$(find_related_output_file "$HOST_OUTPUT_ZH_DIR" "$filename")

    if [ -n "$latest_file" ] && [ -f "$latest_file" ]; then
        log "INFO" "📁 Результирующий файл: $latest_file"
        analyze_markdown_file "$latest_file" "Конвертация"
    else
        log "WARN" "⚠️ Результирующий файл не найден в $HOST_OUTPUT_ZH_DIR"
        log "INFO" "🔍 Поиск любых файлов в выходной папке..."
        find "$HOST_OUTPUT_ZH_DIR" -type f -name "*.md" -mmin -60 2>/dev/null | head -5 | while read -r file; do
            if [ -n "$file" ]; then
                log "INFO" "  Найден: $file"
            fi
        done
    fi
}

process_conversion_batch() {
    log "INFO" "🔍 Поиск PDF файлов для конвертации..."

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

    log "INFO" "📊 Найдено файлов для конвертации: $total_files"
    echo ""

    local processed=0
    local failed=0
    local start_time
    start_time=$(date +%s)

    for pdf_file in "${pdf_files[@]}"; do
        local filename
        filename=$(basename "$pdf_file")
        echo -e "${BLUE}[ФАЙЛ $((processed + failed + 1))/$total_files]${NC} $filename"

        if trigger_full_conversion "$pdf_file"; then
            ((processed++))
            echo -e "Статус: ${GREEN}✅ УСПЕШНО КОНВЕРТИРОВАН${NC}"
        else
            ((failed++))
            echo -e "Статус: ${RED}❌ ОШИБКА КОНВЕРТАЦИИ${NC}"
        fi
        echo ""
    done

    local end_time
    end_time=$(date +%s)
    local total_duration=$((end_time - start_time))

    echo "==============================================================================="
    echo -e "${GREEN}ПОЛНАЯ КОНВЕРТАЦИЯ ЗАВЕРШЕНА${NC}"
    echo "==============================================================================="
    echo -e "📊 Статистика обработки:"
    echo -e " Успешно конвертировано: ${GREEN}$processed${NC} файлов"
    echo -e " Ошибок: ${RED}$failed${NC} файлов"
    echo -e " Общее время: ${BLUE}$total_duration${NC} секунд"
    echo ""
    echo -e "📁 Результаты сохранены в: ${YELLOW}$HOST_OUTPUT_ZH_DIR${NC}"
    echo -e "📋 Логи сохранены в: ${YELLOW}$LOGS_DIR${NC}"
    echo ""

    if [ $failed -gt 0 ]; then
        echo -e "${YELLOW}⚠️ Диагностика проблем:${NC}"
        echo " - Проверьте Airflow UI: $AIRFLOW_URL/dags"
        echo " - Убедитесь что orchestrator_dag активен"
        echo " - Проверьте логи: $LOGS_DIR/conversion_*.log"
        echo " - Проверьте статус всех DAG в проекте"
    else
        echo -e "${GREEN}🎉 Все файлы успешно конвертированы!${NC}"
        echo ""
        echo "Следующие шаги:"
        echo " - Файлы готовы к использованию"
        echo " - Для перевода: ./translate-documents.sh [язык]"
    fi

    return 0
}

convert_only() {
    log "INFO" "🔄 Запуск конвертации PDF → MD (без перевода)"
    echo "==============================================================================="
    echo -e "${CYAN}Режим: Только конвертация (без перевода)${NC}"
    echo -e "📂 Входная папка: ${YELLOW}$HOST_INPUT_DIR${NC}"
    echo -e "📁 Выходная папка: ${YELLOW}$HOST_OUTPUT_ZH_DIR${NC}"
    echo ""
    echo -e "${YELLOW}Нажмите Enter для начала или Ctrl+C для отмены...${NC}"
    read -r

    process_conversion_batch
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
            printf "\r\033[K"
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

        local overview_data
        overview_data=$(fetch_task_overview "$dag_name" "$dag_run_id")
        local progress_line=""

        if [ -n "$overview_data" ] && [ "$overview_data" != "error" ]; then
            IFS='|' read -r total success_count running_count failed_count queued_count retries_count chunks_total chunks_done <<< "$overview_data"

            local total_count=${total:-0}
            if ! [[ "$total_count" =~ ^[0-9]+$ ]]; then
                total_count=0
            fi

            local success_numeric=${success_count:-0}
            local failed_numeric=${failed_count:-0}
            local running_numeric=${running_count:-0}
            local queued_numeric=${queued_count:-0}
            local retries_numeric=${retries_count:-0}
            local chunks_total_numeric=${chunks_total:-0}
            local chunks_done_numeric=${chunks_done:-0}

            if ! [[ "$success_numeric" =~ ^[0-9]+$ ]]; then
                success_numeric=0
            fi

            if ! [[ "$failed_numeric" =~ ^[0-9]+$ ]]; then
                failed_numeric=0
            fi

            if ! [[ "$running_numeric" =~ ^[0-9]+$ ]]; then
                running_numeric=0
            fi

            if ! [[ "$queued_numeric" =~ ^[0-9]+$ ]]; then
                queued_numeric=0
            fi

            if ! [[ "$retries_numeric" =~ ^[0-9]+$ ]]; then
                retries_numeric=0
            fi

            if ! [[ "$chunks_total_numeric" =~ ^[0-9]+$ ]]; then
                chunks_total_numeric=0
            fi

            if ! [[ "$chunks_done_numeric" =~ ^[0-9]+$ ]]; then
                chunks_done_numeric=0
            fi

            local completed=$(( success_numeric + failed_numeric ))

            if [ "$total_count" -le 0 ]; then
                total_count=1
            fi

            local duration_text
            duration_text=$(format_duration "$elapsed")
            local bar
            bar=$(render_progress_bar "$completed" "$total_count" 32)

            progress_line="$bar $duration_text | ✔ $success_numeric • ⏳ $running_numeric • ❌ $failed_numeric • ⏱ $queued_numeric"

            if [ "$retries_numeric" -gt 0 ]; then
                progress_line+=" • 🔁 $retries_numeric"
            fi

            if [ "$chunks_total_numeric" -gt 0 ]; then
                progress_line+=" | чанки: $chunks_done_numeric/$chunks_total_numeric"
            fi
        else
            progress_line="Выполняется ($(format_duration "$elapsed"))"
        fi

        case "$state" in
            "success")
                printf "\r\033[K"
                log "INFO" "✅ Обработка завершена успешно!"
                return 0
                ;;
            "failed"|"upstream_failed")
                printf "\r\033[K"
                log "ERROR" "❌ Обработка завершена с ошибкой"
                return 1
                ;;
            "running"|"queued"|"scheduled")
                printf "\r${YELLOW}[ОБРАБОТКА]${NC} %s" "$progress_line"
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
    prepare_logging "session"

    if ! check_dependencies; then
        echo -e "${RED}❌ Необходимые зависимости отсутствуют. Установите jq и curl.${NC}"
        exit 1
    fi

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
                prepare_logging "translation_ru"
                log "INFO" "Сценарий: Полная обработка PDF → Русский"
                full_pdf_processing "ru" "Русский" "$HOST_OUTPUT_RU_DIR"
                ;;
            2)
                echo -e "${GREEN}Выбран сценарий: Полная обработка PDF → Английский${NC}"
                prepare_logging "translation_en"
                log "INFO" "Сценарий: Полная обработка PDF → Английский"
                full_pdf_processing "en" "Английский" "$HOST_OUTPUT_EN_DIR"
                ;;
            3)
                echo -e "${GREEN}Выбран сценарий: Перевод MD → Русский${NC}"
                prepare_logging "md_translation_ru"
                log "INFO" "Сценарий: Перевод готовых MD → Русский"
                translate_existing_md "ru" "Русский" "$HOST_OUTPUT_RU_DIR"
                ;;
            4)
                echo -e "${GREEN}Выбран сценарий: Перевод MD → Английский${NC}"
                prepare_logging "md_translation_en"
                log "INFO" "Сценарий: Перевод готовых MD → Английский"
                translate_existing_md "en" "Английский" "$HOST_OUTPUT_EN_DIR"
                ;;
            5)
                echo -e "${GREEN}Выбран сценарий: Только конвертация PDF → MD${NC}"
                prepare_logging "conversion"
                log "INFO" "Сценарий: Только конвертация PDF → Markdown"
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
