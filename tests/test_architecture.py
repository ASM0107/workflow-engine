import ast
import tokenize
from pathlib import Path


def test_zero_comments_in_source_code() -> None:
    source_root = Path(__file__).resolve().parent.parent / "backend" / "src" / "workflow_engine"
    python_files = list(source_root.rglob("*.py"))
    assert len(python_files) > 0

    comment_violations: list[str] = []

    for file_path in python_files:
        with open(file_path, "rb") as f:
            tokens = tokenize.tokenize(f.readline)
            for token in tokens:
                if token.type == tokenize.COMMENT:
                    comment_violations.append(
                        f"{file_path.name}:{token.start[0]} - {token.string}"
                    )

    assert not comment_violations, f"Found comments in source code: {comment_violations}"


def test_zero_docstrings_in_source_code() -> None:
    source_root = Path(__file__).resolve().parent.parent / "backend" / "src" / "workflow_engine"
    python_files = list(source_root.rglob("*.py"))
    assert len(python_files) > 0

    docstring_violations: list[str] = []

    for file_path in python_files:
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=str(file_path))
            for node in ast.walk(tree):
                if isinstance(
                    node,
                    (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module),
                ):
                    docstring = ast.get_docstring(node)
                    if docstring:
                        docstring_violations.append(
                            f"{file_path.name} ({type(node).__name__}): {docstring[:30]}..."
                        )

    assert not docstring_violations, f"Found docstrings in source code: {docstring_violations}"
