#!/usr/bin/env python3
"""
render_pdf.py — скрипт конвертации Markdown в PDF
с поддержкой китайских шрифтов и пользовательских шаблонов.
"""
import os, sys, json, subprocess, datetime

LOG_BASE = "/app/logs"
LOG_FILE = None

def get_log_file():
    global LOG_FILE
    if LOG_FILE is None:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        LOG_FILE = os.path.join(LOG_BASE, f"pandoc_render_{ts}.log")
    return LOG_FILE

def write_log(msg):
    logpath = get_log_file()
    msg_dt = f"[{datetime.datetime.now().isoformat()}] {msg}"
    try:
        with open(logpath, "a", encoding="utf-8") as f:
            f.write(msg_dt + "\n")
    except Exception as e:
        # Fallback на stderr
        print(f"LOG_ERR:{e}")
    print(msg_dt)

def render_markdown_to_pdf(input_md, output_pdf, template=None):
    try:
        # Базовая команда Pandoc
        cmd = [
            'pandoc', input_md,
            '-o', output_pdf,
            '--pdf-engine=xelatex',
            '--variable', 'CJKmainfont=Noto Sans CJK SC',
            '--variable', 'geometry:margin=2cm',
            '--variable', 'fontsize=11pt',
            '--variable', 'linestretch=1.2',
            '--table-of-contents',
            '--number-sections'
        ]
        # Добавление шаблона, если передан
        if template and os.path.exists(template):
            cmd.extend(['--template', template])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        # Логируем stdout/stderr всегда
        if result.stdout:
            write_log(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            write_log(f"STDERR:\n{result.stderr}")

        if result.returncode == 0:
            write_log(f"SUCCESS: PDF generated at {output_pdf}")
            return {'success': True, 'output_file': output_pdf}
        else:
            write_log(f"ERROR: Pandoc failed with code {result.returncode}")
            return {'success': False, 'error': result.stderr, 'log_file': get_log_file()}
    except subprocess.TimeoutExpired:
        write_log("TIMEOUT: PDF rendering exceeded time limit")
        return {'success': False, 'error': 'Timeout during PDF rendering', 'log_file': get_log_file()}
    except Exception as e:
        write_log(f"FATAL: Exception {e}")
        return {'success': False, 'error': str(e), 'log_file': get_log_file()}

def main():
    if len(sys.argv) < 3:
        print("Usage: render_pdf.py <input.md> <output.pdf> [template]")
        sys.exit(1)
    input_md, output_pdf = sys.argv[1], sys.argv[2]
    template = sys.argv[3] if len(sys.argv) > 3 else None
    if not os.path.exists(input_md):
        err = f"Error: {input_md} not found"
        write_log(err)
        print(err)
        sys.exit(1)
    os.makedirs(os.path.dirname(output_pdf), exist_ok=True)
    result = render_markdown_to_pdf(input_md, output_pdf, template)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    sys.exit(0 if result.get('success') else 1)

if __name__ == "__main__":
    main()