#!/usr/bin/env python3
"""Incremental ETL pipeline.

Reads CSV rows from INPUT, applies one transformation and one validation
rule, appends surviving rows to OUTPUT, records processed ids in STATE,
and prints a concise run summary to stdout.
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import tempfile
from decimal import Decimal, InvalidOperation

AMOUNT_RE = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?")


def eprint(msg):
    print(msg, file=sys.stderr)


def is_blank_physical_line(line):
    """Return True when a single physical line is empty or spaces/tabs only.

    The line break itself is excluded from the check.
    """
    content = line.rstrip("\r\n")
    if content == "":
        return True
    for ch in content:
        if ch not in (" ", "\t"):
            return False
    return True


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Incremental ETL pipeline."
    )
    parser.add_argument("--input", required=True, help="Input CSV file.")
    parser.add_argument("--output", required=True, help="Output CSV file.")
    parser.add_argument("--state", required=True, help="JSON state file.")
    return parser.parse_args(argv)


def fail_usage(msg):
    eprint(f"error: {msg}")
    return 2


def fail_runtime(msg):
    eprint(f"error: {msg}")
    return 1


def check_writable_existing(path):
    """Check an existing path is writable without modifying its content."""
    try:
        if os.path.isdir(path):
            return False
        with open(path, "a", encoding="utf-8", newline=""):
            pass
        return True
    except OSError:
        return False


def check_writable_missing(path):
    """Check a missing path could be created without creating it."""
    parent = os.path.dirname(os.path.abspath(path))
    try:
        if not os.path.isdir(parent):
            return False
        fd, tmp = tempfile.mkstemp(dir=parent)
        try:
            os.close(fd)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return True
    except OSError:
        return False


def ensure_writable(path):
    if os.path.exists(path):
        return check_writable_existing(path)
    return check_writable_missing(path)


def main(argv=None):
    args = parse_args(argv)
    input_path = args.input
    output_path = args.output
    state_path = args.state

    # --- Read input file (usage error on failure) ---
    try:
        with open(input_path, "r", encoding="utf-8", newline="") as f:
            raw_lines = f.readlines()
    except OSError as exc:
        return fail_usage(f"cannot read input file '{input_path}': {exc}")

    text = "".join(raw_lines)
    reader = csv.reader(io.StringIO(text))

    header = None
    data_records = []  # list of row lists (non-blank, in order)
    prev_line = 0
    try:
        for row in reader:
            cur_line = reader.line_num
            chunk = raw_lines[prev_line:cur_line]
            prev_line = cur_line
            if len(chunk) == 1 and is_blank_physical_line(chunk[0]):
                continue
            if header is None:
                header = row
                continue
            data_records.append(row)
    except (csv.Error, UnicodeDecodeError) as exc:
        return fail_usage(f"cannot parse input file '{input_path}': {exc}")
    except OSError as exc:
        return fail_usage(f"cannot read input file '{input_path}': {exc}")

    if header is None:
        return fail_usage(
            f"input file '{input_path}' is missing header "
            "'id,name,amount'"
        )
    if header != ["id", "name", "amount"]:
        return fail_usage(
            f"invalid header in '{input_path}': expected 'id,name,amount'"
        )

    # --- Read state file (runtime error on corrupt) ---
    if not os.path.exists(state_path):
        initial_ids = []
    else:
        try:
            if os.path.isdir(state_path):
                raise OSError(f"'{state_path}' is a directory")
            with open(state_path, "r", encoding="utf-8", newline="") as sf:
                state_data = json.load(sf)
        except json.JSONDecodeError as exc:
            return fail_runtime(f"corrupt state file '{state_path}': {exc}")
        except UnicodeDecodeError as exc:
            return fail_runtime(f"corrupt state file '{state_path}': {exc}")
        except OSError as exc:
            return fail_runtime(
                f"cannot read state file '{state_path}': {exc}"
            )
        if (
            not isinstance(state_data, dict)
            or "processed_ids" not in state_data
            or not isinstance(state_data["processed_ids"], list)
            or not all(
                isinstance(x, str) for x in state_data["processed_ids"]
            )
        ):
            return fail_runtime(
                f"corrupt state file '{state_path}': "
                "expected object {'processed_ids': [...strings...]}"
            )
        initial_ids = list(state_data["processed_ids"])

    # --- Process rows in memory ---
    seen = set(initial_ids)
    new_ids_in_order = []
    output_rows = []  # list of [id, name, amount, cents_str]

    processed = len(data_records)
    new_count = 0
    written_count = 0
    invalid_count = 0

    for row in data_records:
        # Determine the id used for dedup/state (None means "adds no id").
        if len(row) != 3:
            if len(row) >= 1 and row[0] != "":
                row_id = row[0]
            else:
                row_id = None
        else:
            row_id = row[0]

        if row_id is not None and row_id in seen:
            continue

        # New row (no-id rows are always new).
        new_count += 1
        if row_id is not None:
            seen.add(row_id)
            new_ids_in_order.append(row_id)

        # Validate.
        valid = True
        cents_str = None
        if len(row) != 3:
            valid = False
        else:
            rid, rname, amount_str = row
            if rid == "":
                valid = False
            elif AMOUNT_RE.fullmatch(amount_str) is None:
                valid = False
            else:
                try:
                    if Decimal(amount_str) < 0:
                        valid = False
                    else:
                        cents = int(round(Decimal(amount_str) * 100))
                        cents_str = str(cents)
                except (InvalidOperation, ValueError, ArithmeticError):
                    valid = False

        if not valid:
            invalid_count += 1
            continue

        written_count += 1
        output_rows.append([row[0], row[1], row[2], cents_str])

    final_ids = initial_ids + new_ids_in_order

    # --- Pre-check writability before modifying anything ---
    if not ensure_writable(output_path):
        return fail_runtime(f"cannot write output file '{output_path}'")
    if not ensure_writable(state_path):
        return fail_runtime(f"cannot write state file '{state_path}'")

    # Snapshot for rollback so a second failure leaves both files unchanged.
    output_existed = os.path.exists(output_path)
    state_existed = os.path.exists(state_path)
    output_old_bytes = None
    state_old_bytes = None
    try:
        if output_existed:
            with open(output_path, "rb") as f:
                output_old_bytes = f.read()
        if state_existed:
            with open(state_path, "rb") as f:
                state_old_bytes = f.read()
    except OSError as exc:
        return fail_runtime(f"cannot read output/state for update: {exc}")

    def restore_output():
        try:
            if not output_existed:
                try:
                    os.unlink(output_path)
                except OSError:
                    pass
            elif output_old_bytes is not None:
                with open(output_path, "wb") as f:
                    f.write(output_old_bytes)
        except OSError:
            pass

    def restore_state():
        try:
            if not state_existed:
                try:
                    os.unlink(state_path)
                except OSError:
                    pass
            elif state_old_bytes is not None:
                with open(state_path, "wb") as f:
                    f.write(state_old_bytes)
        except OSError:
            pass

    # --- Write output ---
    try:
        if not output_existed or (output_old_bytes is not None and len(output_old_bytes) == 0):
            with open(output_path, "w", encoding="utf-8", newline="") as of:
                writer = csv.writer(of, lineterminator="\n")
                writer.writerow(["id", "name", "amount", "amount_cents"])
                writer.writerows(output_rows)
        else:
            needs_newline = False
            if output_old_bytes is not None and len(output_old_bytes) > 0:
                if not output_old_bytes.endswith(b"\n"):
                    needs_newline = True
            with open(output_path, "a", encoding="utf-8", newline="") as of:
                if needs_newline:
                    of.write("\n")
                writer = csv.writer(of, lineterminator="\n")
                writer.writerows(output_rows)
    except OSError as exc:
        restore_output()
        return fail_runtime(f"cannot write output file '{output_path}': {exc}")

    # --- Write state ---
    try:
        state_payload = {"processed_ids": final_ids}
        with open(state_path, "w", encoding="utf-8", newline="") as sf:
            json.dump(state_payload, sf, ensure_ascii=False, indent=2)
            sf.write("\n")
    except OSError as exc:
        restore_output()
        restore_state()
        return fail_runtime(f"cannot write state file '{state_path}': {exc}")

    # --- Summary ---
    sys.stdout.write(
        f"processed={processed} new={new_count} "
        f"written={written_count} invalid={invalid_count}\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
