import subprocess
from pathlib import Path


def test_find_related_output_file_accepts_embedded_stem(tmp_path):
    base = "example"
    fake_file = tmp_path / f"12345_{base}.md"
    fake_file.write_text("# Test document\n")

    script_path = Path(__file__).resolve().parents[2] / "translate-documents.sh"

    command = (
        f"source '{script_path}'"
        f"; find_related_output_file '{tmp_path}' '{base}.pdf'"
    )

    result = subprocess.run(
        ["bash", "-lc", command],
        check=True,
        text=True,
        capture_output=True,
    )

    assert result.stdout.strip() == str(fake_file)
