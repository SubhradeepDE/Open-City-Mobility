from typing import Any


def check_not_empty(value: Any, field_name: str) -> tuple[bool, str]:
    """Check that a value is not None or an empty string."""
    if value is None:
        return False, f"{field_name} is null"

    if isinstance(value, str) and not value.strip():
        return False, f"{field_name} is empty"

    return True, ""


def check_non_negative(
    value: Any,
    field_name: str,
) -> tuple[bool, str]:
    """Check that a numeric value is zero or greater."""
    if value is None:
        return False, f"{field_name} is null"

    if value < 0:
        return False, f"{field_name} must be non-negative"

    return True, ""


def check_range(
    value: Any,
    field_name: str,
    minimum: float,
    maximum: float,
) -> tuple[bool, str]:
    """Check that a numeric value falls within a valid range."""
    if value is None:
        return False, f"{field_name} is null"

    if not minimum <= value <= maximum:
        return (
            False,
            f"{field_name} must be between {minimum} and {maximum}",
        )

    return True, ""


def check_required_columns(
    columns: list[str],
    required_columns: list[str],
) -> tuple[bool, str]:
    """Check that all required columns exist."""
    missing = [
        column
        for column in required_columns
        if column not in columns
    ]

    if missing:
        return False, f"Missing columns: {', '.join(missing)}"

    return True, ""