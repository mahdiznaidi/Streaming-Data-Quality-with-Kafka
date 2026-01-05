import io
import json
import pathlib
import sys

# Ensure repository root is on sys.path for module imports
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import consumer


def test_parse_message_invalid_json():
    payload, errors = consumer.parse_message("{invalid}")

    assert payload is None
    assert errors and errors[0].startswith("invalid_json")


def test_validate_payload_with_missing_fields():
    result = consumer.validate_payload({"ORIGIN_COUNTRY_NAME": "US"})

    assert not result.is_valid
    assert "missing_field:DEST_COUNTRY_NAME" in result.reasons
    assert "missing_field:count" in result.reasons


def test_process_stream_routes_valid_and_invalid_records():
    lines = [
        json.dumps(
            {
                "ORIGIN_COUNTRY_NAME": "United States",
                "DEST_COUNTRY_NAME": "Canada",
                "count": 5,
            }
        ),
        json.dumps({"ORIGIN_COUNTRY_NAME": "", "DEST_COUNTRY_NAME": "Canada"}),
        "not-json",
    ]

    valid_buffer = io.StringIO()
    invalid_buffer = io.StringIO()

    valid_writer = consumer.TopicWriter(valid_buffer)
    invalid_writer = consumer.TopicWriter(invalid_buffer)

    result = consumer.process_stream(lines, valid_writer, invalid_writer)

    assert result.valid_count == 1
    assert result.invalid_count == 2

    valid_buffer.seek(0)
    invalid_buffer.seek(0)

    valid_lines = [json.loads(line) for line in valid_buffer.readlines()]
    invalid_lines = [json.loads(line) for line in invalid_buffer.readlines()]

    assert valid_lines == [
        {
            "ORIGIN_COUNTRY_NAME": "United States",
            "DEST_COUNTRY_NAME": "Canada",
            "count": 5,
        }
    ]

    assert invalid_lines[0]["payload"]["ORIGIN_COUNTRY_NAME"] == ""
    assert "invalid_origin" in invalid_lines[0]["errors"]
    assert "missing_count" in invalid_lines[0]["errors"]
    assert invalid_lines[1]["raw"] == "not-json"
    assert invalid_lines[1]["errors"][0].startswith("invalid_json")
