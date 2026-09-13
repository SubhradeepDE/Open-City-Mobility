from src.quality.checks import (
    check_not_empty,
    check_non_negative,
    check_range,
    check_required_columns,
)
from src.quality.runner import all_checks_passed, run_checks


def test_quality_checks():
    checks = [
        {
            "name": "route_id_not_empty",
            "function": check_not_empty,
            "value": "123",
            "field_name": "route_id",
        },
        {
            "name": "vehicle_count_non_negative",
            "function": check_non_negative,
            "value": 10,
            "field_name": "vehicle_count",
        },
        {
            "name": "latitude_valid",
            "function": check_range,
            "value": 28.6139,
            "field_name": "latitude",
            "minimum": -90,
            "maximum": 90,
        },
        {
            "name": "required_columns",
            "function": check_required_columns,
            "value": [
                "route_id",
                "vehicle_count",
            ],
            "required_columns": [
                "route_id",
                "vehicle_count",
            ],
        },
    ]

    results = run_checks(checks)

    for result in results:
        print(result)

    assert all_checks_passed(results)