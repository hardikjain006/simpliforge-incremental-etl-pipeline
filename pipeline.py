#!/usr/bin/env python3
"""Incremental ETL pipeline.

Reads ``data/input.csv``, applies exactly one transformation (``name``
normalization), enforces exactly one validation rule (``amount`` must be a
finite decimal number greater than or equal to zero), and writes the single
output ``data/output.csv``. See README.md for the full requirements contract.

Usage (from the repository root)::

    python3 pipeline.py [--input data/input.csv] [--output data/output.csv]

A run summary (rows read, rows written, rows skipped, new rows) is printed to
standard output. Hard errors (missing/unreadable input, bad header) are
reported on standard error with a non-zero exit and no partial output.
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import tempfile
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext

STATE_FILENAME = ".etl_state.json"

ASCII_WS = " \t\n\r\x0b\x0c"
_WS_RUN_RE = re.compile("[ \t\n\r\x0b\x0c]+")
_ID_RE = re.compile("[+-]?[0-9]+")
_AMOUNT_RE = re.compile("[+-]?[0-9]+(?:[.][0-9]+)?")

EXPECTED_HEADER = ["id", "name", "amount"]


def normalize_name(raw):
    """Strip leading/trailing ASCII whitespace and collapse each interior run
    of ASCII whitespace to a single space. Letter case is preserved."""
    return _WS_RUN_RE.sub(" ", raw.strip(ASCII_WS))


def parse_id(raw):
    """Return the canonical decimal rendering of an integer ``id`` field, or
    ``None`` when the field is not an integer after ASCII-whitespace trim."""
    text = raw.strip(ASCII_WS)
    if _ID_RE.fullmatch(text) is None:
        return None
    try:
        value = int(text)
    except ValueError:
        return None
    return value


def parse_amount(raw):
    """Return the non-negative ``Decimal`` value of an ``amount`` field, or
    ``None`` when it fails the validation rule."""
    text = raw.strip(ASCII_WS)
    if _AMOUNT_RE.fullmatch(text) is None:
        return None
    try:
        with localcontext() as ctx:
            ctx.prec = max(len(text) + 10, 28)
            value = Decimal(text)
    except InvalidOperation:
        return None
    if value < 0:
        return None
    return value


def format_amount(value):
    """Format a validated amount with exactly two decimal places, rounding
    half away from zero. Zero (including signed zero) renders as ``0.00``."""
    with localcontext() as ctx:
        ctx.prec = max(len(str(value)) + 10, 28)
        quantized = value.quantize(Decimal("0.01"),
                                    rounding=ROUND_HALF_UP)
    if quantized == 0:
        return "0.00"
    return str(quantized)


def read_input_rows(path):
    """Return ``(header_ok, data_rows)``. Raises ``OSError`` when unreadable."""
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))
    if not rows or rows[0] != EXPECTED_HEADER:
        return False, []
    return True, rows[1:]


def process_rows(data_rows):
    """Apply the transformation and validation rule.

    Returns ``(output_rows, read_count, skipped_count)`` where each output
    row is ``(id_int, seq, id_text, name, amount_text)`` sorted by ascending
    numeric ``id`` (stable within equal ``id`` groups).
    """
    output_rows = []
    skipped = 0
    for seq, row in enumerate(data_rows):
        if len(row) != len(EXPECTED_HEADER):
            skipped += 1
            continue
        id_raw, name_raw, amount_raw = row
        id_value = parse_id(id_raw)
        amount_value = parse_amount(amount_raw)
        if id_value is None or amount_value is None:
            skipped += 1
            continue
        output_rows.append(
            (id_value, seq, str(id_value), normalize_name(name_raw),
             format_amount(amount_value))
        )
    output_rows.sort(key=lambda item: item[0])
    return output_rows, len(data_rows), skipped


def write_output_csv(path, output_rows):
    """Write the output CSV atomically (temp file + rename)."""
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=parent, prefix=".output.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(EXPECTED_HEADER)
            for _, _, id_text, name, amount_text in output_rows:
                writer.writerow([id_text, name, amount_text])
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_state(state_path):
    try:
        with open(state_path, "r", encoding="utf-8") as handle:
            state = json.load(handle)
        if (isinstance(state, dict)
                and isinstance(state.get("input_sha256"), str)
                and isinstance(state.get("rows_written"), int)):
            return state
    except (OSError, ValueError):
        pass
    return None


def save_state(state_path, input_sha256, read_count, written_count,
               skipped_count):
    parent = os.path.dirname(os.path.abspath(state_path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=parent, prefix=".state.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                {"input_sha256": input_sha256,
                 "rows_read": read_count,
                 "rows_written": written_count,
                 "rows_skipped": skipped_count},
                handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, state_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def run(input_path, output_path, state_path):
    """Execute one pipeline run. Returns ``(exit_code, message)`` where the
    message goes to stdout on success and stderr on failure."""
    try:
        with open(input_path, "rb") as handle:
            input_bytes = handle.read()
    except OSError as exc:
        return (1, "etl error: cannot read input file %r: %s"
                % (input_path, exc.strerror or exc))
    try:
        header_ok, data_rows = read_input_rows(input_path)
    except OSError as exc:
        return (1, "etl error: cannot read input file %r: %s"
                % (input_path, exc.strerror or exc))
    if not header_ok:
        return (1, "etl error: input file %r must start with exactly the "
                "header row `id,name,amount`" % (input_path,))

    output_rows, read_count, skipped_count = process_rows(data_rows)
    written_count = len(output_rows)
    input_sha256 = hashlib.sha256(input_bytes).hexdigest()

    previous = load_state(state_path)
    unchanged = (previous is not None
                 and previous.get("input_sha256") == input_sha256
                 and os.path.exists(output_path))
    if unchanged:
        new_count = 0
    elif previous is not None:
        new_count = max(0, written_count - previous.get("rows_written", 0))
    else:
        new_count = written_count

    if not unchanged:
        try:
            write_output_csv(output_path, output_rows)
        except OSError as exc:
            return (1, "etl error: cannot write output file %r: %s"
                    % (output_path, exc.strerror or exc))
        try:
            save_state(state_path, input_sha256, read_count, written_count,
                       skipped_count)
        except OSError as exc:
            return (1, "etl error: cannot write state file %r: %s"
                    % (state_path, exc.strerror or exc))

    return (0, "etl: rows read=%d rows written=%d rows skipped=%d new rows=%d "
            "input=%s output=%s"
            % (read_count, written_count, skipped_count, new_count,
               input_path, output_path))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", default="data/input.csv")
    parser.add_argument("--output", default="data/output.csv")
    parser.add_argument("--state", default=STATE_FILENAME)
    args = parser.parse_args(argv)
    exit_code, message = run(args.input, args.output, args.state)
    print(message, file=sys.stdout if exit_code == 0 else sys.stderr)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
