"""Helper functions for tap-fred."""

import json
import re
import uuid


def to_snake_case(string: str) -> str:
    """Convert a camelCase or PascalCase string to snake_case."""
    if not string:
        return string

    string = re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()
    string = re.sub(r"[^a-zA-Z0-9_]", "_", string)
    string = re.sub(r"_+", "_", string)
    return string.strip("_")


# Backwards-compatible alias
clean_strings = to_snake_case


def clean_json_keys(data):
    """Recursively convert all JSON keys to snake_case."""
    if isinstance(data, dict):
        return {
            to_snake_case(key): clean_json_keys(value) for key, value in data.items()
        }
    if isinstance(data, list):
        return [clean_json_keys(item) for item in data]
    return data


def generate_surrogate_key(data: dict, namespace=uuid.NAMESPACE_DNS) -> str:
    """Generate a UUID5 surrogate key from all record field values."""
    key_string = "|".join(str(data.get(f, "")) for f in sorted(data.keys()))
    return str(uuid.uuid5(namespace, key_string))


def join_tag_names(tag_names) -> str:
    """Convert tag_names config (list or string) to FRED API semicolon-delimited format.

    FRED API requires tag_names as semicolon-separated string (e.g., "gdp;inflation").
    """
    if isinstance(tag_names, list):
        return ";".join(tag_names)
    if isinstance(tag_names, str):
        return tag_names
    raise ValueError(
        f"tag_names must be a list or string, got {type(tag_names).__name__}"
    )


def coerce_str_to_bool(value) -> bool:
    """Convert a FRED API string boolean ("true"/"false") to Python bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def normalize_config_list(value) -> list[str]:
    """Normalize a config value to a clean list of string IDs.

    Handles all the ways IDs can arrive from env vars / meltano / Singer SDK:
    - Already a list: ``["GDP", "UNRATE"]`` -> passthrough
    - JSON-encoded string: ``'["GDP","UNRATE"]'`` -> decoded to list
    - Plain string: ``"GDP"`` -> wrapped as ``["GDP"]``
    - Double-encoded elements: ``['["GDP"]', "UNRATE"]`` -> ``["GDP", "UNRATE"]``
    """
    # Decode outer string to list
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            if isinstance(decoded, list):
                value = decoded
            else:
                value = [value]
        except (json.JSONDecodeError, TypeError):
            value = [value]

    if not isinstance(value, list):
        return [str(value)]

    # Unwrap double-encoded elements
    clean: list[str] = []
    for item in value:
        s = str(item).strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                inner = json.loads(s)
                if isinstance(inner, list):
                    clean.extend(str(x) for x in inner)
                    continue
            except (json.JSONDecodeError, TypeError):
                pass
        clean.append(s)
    return clean
