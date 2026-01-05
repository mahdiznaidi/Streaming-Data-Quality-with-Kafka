"""Kafka consumer simulation for validating flight summary data.

This module reads newline-delimited JSON input and validates each
record for schema and data quality issues. Valid records are written to
one sink while invalid records are written to a separate sink with
reasons for rejection. The implementation avoids external dependencies
so it can run in constrained environments without Kafka brokers.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, TextIO, Tuple

EXPECTED_FIELDS = ["ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count"]


@dataclass
class ValidationResult:
    """Outcome of validating a single record."""

    is_valid: bool
    reasons: List[str] = field(default_factory=list)


def parse_message(raw: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Safely parse a JSON message.

    Returns a tuple of the parsed payload (or ``None`` if parsing fails)
    and a list of parsing errors.
    """

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, [f"invalid_json:{exc.msg}"]

    if not isinstance(payload, dict):
        return None, ["payload_not_object"]

    return payload, []


def validate_payload(payload: Dict[str, Any]) -> ValidationResult:
    """Validate required fields and types for a payload."""

    reasons: List[str] = []

    for field_name in EXPECTED_FIELDS:
        if field_name not in payload:
            reasons.append(f"missing_field:{field_name}")

    origin = payload.get("ORIGIN_COUNTRY_NAME")
    dest = payload.get("DEST_COUNTRY_NAME")
    count = payload.get("count")

    if origin is None or not isinstance(origin, str) or not origin.strip():
        reasons.append("invalid_origin")

    if dest is None or not isinstance(dest, str) or not dest.strip():
        reasons.append("invalid_destination")

    if isinstance(count, bool) or not isinstance(count, (int, type(None))):
        reasons.append("invalid_count_type")
    elif isinstance(count, int):
        if count < 0:
            reasons.append("negative_count")
    else:
        # ``count`` is None
        reasons.append("missing_count")

    return ValidationResult(is_valid=len(reasons) == 0, reasons=reasons)


@dataclass
class RoutingResult:
    """Summary of routed messages."""

    valid_count: int = 0
    invalid_count: int = 0


class TopicWriter:
    """Simple sink that writes messages to a JSONL file."""

    def __init__(self, stream: TextIO):
        self.stream = stream

    def write(self, message: Dict[str, Any]) -> None:
        self.stream.write(json.dumps(message))
        self.stream.write("\n")
        self.stream.flush()


def route_record(
    payload: Dict[str, Any],
    validation: ValidationResult,
    valid_writer: TopicWriter,
    invalid_writer: TopicWriter,
) -> None:
    """Route a record to the appropriate sink based on validation."""

    if validation.is_valid:
        valid_writer.write(payload)
    else:
        invalid_writer.write({"payload": payload, "errors": validation.reasons})


def process_stream(
    lines: Iterable[str],
    valid_writer: TopicWriter,
    invalid_writer: TopicWriter,
) -> RoutingResult:
    """Process an iterable of raw messages and route them appropriately."""

    result = RoutingResult()

    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue

        payload, errors = parse_message(raw)
        if payload is None:
            invalid_writer.write({"raw": raw, "errors": errors})
            result.invalid_count += 1
            continue

        validation = validate_payload(payload)
        route_record(payload, validation, valid_writer, invalid_writer)
        if validation.is_valid:
            result.valid_count += 1
        else:
            result.invalid_count += 1

    return result


def read_lines_from_file(path: str) -> Iterable[str]:
    with open(path, "r", encoding="utf-8") as stream:
        for line in stream:
            yield line


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate streaming flight data")
    parser.add_argument(
        "--input",
        default="flights_summary.json",
        help="Path to newline-delimited JSON data (default: flights_summary.json)",
    )
    parser.add_argument(
        "--valid-output",
        default="valid_records.jsonl",
        help="File to write validated records to",
    )
    parser.add_argument(
        "--invalid-output",
        default="invalid_records.jsonl",
        help="File to write invalid records and error reasons to",
    )

    args = parser.parse_args()

    with open(args.valid_output, "w", encoding="utf-8") as valid_stream, open(
        args.invalid_output, "w", encoding="utf-8"
    ) as invalid_stream:
        valid_writer = TopicWriter(valid_stream)
        invalid_writer = TopicWriter(invalid_stream)

        summary = process_stream(
            read_lines_from_file(args.input),
            valid_writer,
            invalid_writer,
        )

    print(
        f"Validation complete. Valid: {summary.valid_count} | Invalid: {summary.invalid_count}"
    )


if __name__ == "__main__":
    main()
