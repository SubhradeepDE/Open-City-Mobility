from typing import Any, Callable


QualityCheck = Callable[..., tuple[bool, str]]


class QualityResult:
    """Stores the result of a single data-quality check."""

    def __init__(
        self,
        check_name: str,
        passed: bool,
        message: str = "",
    ) -> None:
        self.check_name = check_name
        self.passed = passed
        self.message = message

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"

        if self.message:
            return f"{status}: {self.check_name} - {self.message}"

        return f"{status}: {self.check_name}"


def run_check(
    check_name: str,
    check_function: QualityCheck,
    value: Any,
    **kwargs: Any,
) -> QualityResult:
    """
    Execute one data-quality check and return a standardized result.
    """
    try:
        passed, message = check_function(
            value,
            **kwargs,
        )

        return QualityResult(
            check_name=check_name,
            passed=passed,
            message=message,
        )

    except Exception as exc:
        return QualityResult(
            check_name=check_name,
            passed=False,
            message=f"Check execution failed: {exc}",
        )


def run_checks(
    checks: list[dict[str, Any]],
) -> list[QualityResult]:
    """
    Execute multiple data-quality checks.

    Example:
        checks = [
            {
                "name": "route_id_not_empty",
                "function": check_not_empty,
                "value": "123",
                "field_name": "route_id",
            }
        ]
    """
    results: list[QualityResult] = []

    for check in checks:
        result = run_check(
            check_name=check["name"],
            check_function=check["function"],
            value=check["value"],
            **{
                key: value
                for key, value in check.items()
                if key not in {"name", "function", "value"}
            },
        )

        results.append(result)

    return results


def all_checks_passed(
    results: list[QualityResult],
) -> bool:
    """Return True only when every quality check passes."""
    return all(result.passed for result in results)