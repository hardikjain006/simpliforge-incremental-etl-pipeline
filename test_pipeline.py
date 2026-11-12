#!/usr/bin/env python3
"""Repository-native tests for the incremental ETL pipeline.

Run from the repository root::

    python3 -m unittest test_pipeline -v

Every test runs the pipeline as a subprocess in an isolated temporary
directory, so the repository's own ``data/output.csv`` and state file are
never touched.
"""

import csv
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

PIPELINE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "pipeline.py")
HEADER = ["id", "name", "amount"]


def write_input(root, rows, header=HEADER, raw_text=None):
    data_dir = os.path.join(root, "data")
    os.makedirs(data_dir, exist_ok=True)
    path = os.path.join(data_dir, "input.csv")
    if raw_text is not None:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            handle.write(raw_text)
    else:
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)
    return path


def read_output(root):
    path = os.path.join(root, "data", "output.csv")
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return path, list(csv.reader(handle))


def output_bytes(root):
    with open(os.path.join(root, "data", "output.csv"), "rb") as handle:
        return handle.read()


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="etl-test-")
        self.addCleanup(shutil.rmtree, self.root, True)

    def run_pipeline(self, *extra_args):
        return subprocess.run(
            [sys.executable, PIPELINE] + list(extra_args),
            cwd=self.root, capture_output=True, text=True, timeout=120)

    def test_reference_fixture_shape(self):
        """The committed 100-row fixture is all-valid: one run writes 100
        ordered rows with normalized names and two-decimal amounts."""
        fixture = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "data", "input.csv")
        with open(fixture, "r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))
        self.assertEqual(rows[0], HEADER)
        self.assertEqual(len(rows) - 1, 100)
        write_input(self.root, rows[1:])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual(out[0], HEADER)
        body = out[1:]
        self.assertEqual(len(body), 100)
        ids = [int(row[0]) for row in body]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(ids, list(range(1, 101)))
        for row in body:
            self.assertEqual(len(row), 3)
            self.assertNotRegex(row[1], r"  |\t")
            self.assertEqual(row[1], row[1].strip(" \t"))
            self.assertRegex(row[2], r"^-?[0-9]+\.[0-9]{2}$")

    def test_invalid_amounts_skipped_and_counted(self):
        write_input(self.root, [
            ("1", "ok", "10"),
            ("2", "neg", "-3"),
            ("3", "text", "abc"),
            ("4", "sci", "1e3"),
            ("5", "inf", "Infinity"),
            ("6", "nan", "NaN"),
            ("7", "empty", ""),
            ("8", "blank", "   "),
            ("9", "dot lead", ".5"),
            ("10", "dot trail", "5."),
            ("11", "ok2", "0"),
        ])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual([row[0] for row in out[1:]], ["1", "11"])
        self.assertIn("rows read=11", proc.stdout)
        self.assertIn("rows written=2", proc.stdout)
        self.assertIn("rows skipped=9", proc.stdout)

    def test_signed_zero_valid_and_renders_plain(self):
        write_input(self.root, [
            ("1", "a", "-0"),
            ("2", "b", "-0.00"),
            ("3", "c", "0"),
            ("4", "d", " 0.00 "),
        ])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual([row[2] for row in out[1:]],
                         ["0.00", "0.00", "0.00", "0.00"])

    def test_rounding_half_away_from_zero(self):
        write_input(self.root, [
            ("1", "a", "12.345"),
            ("2", "b", "2.675"),
            ("3", "c", "7"),
        ])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual([row[2] for row in out[1:]],
                         ["12.35", "2.68", "7.00"])

    def test_name_normalization_and_canonical_id(self):
        write_input(self.root, [
            (" 007 ", "  hello\t\tworld  ", "1"),
            ("+8", "MiXeD   \t Case", "2"),
        ])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual(out[1], ["7", "hello world", "1.00"])
        self.assertEqual(out[2], ["8", "MiXeD Case", "2.00"])

    def test_comma_and_quote_names_quoted(self):
        write_input(self.root, [
            ("1", "a,b", "1"),
            ("2", 'say "hi"', "2"),
            ("3", "plain", "3"),
        ])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        raw = output_bytes(self.root).decode("utf-8")
        self.assertIn('"a,b"', raw)
        self.assertIn('"say ""hi"""', raw)
        _, out = read_output(self.root)
        self.assertEqual(out[1][1], "a,b")
        self.assertEqual(out[2][1], 'say "hi"')

    def test_empty_name_still_processed(self):
        write_input(self.root, [("1", "   ", "5")])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual(out[1], ["1", "", "5.00"])

    def test_duplicate_ids_kept_stably(self):
        write_input(self.root, [
            ("2", "second", "1"),
            ("1", "first", "1"),
            ("2", "third", "1"),
        ])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual([row[1] for row in out[1:]],
                         ["first", "second", "third"])

    def test_malformed_rows_skipped(self):
        write_input(self.root, [], raw_text=(
            "id,name,amount\n"
            "1,good,5\n"
            "2,too,many,cols,here\n"
            "3,short\n"
            "4,also good,6\n"))
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual([row[0] for row in out[1:]], ["1", "4"])
        self.assertIn("rows skipped=2", proc.stdout)

    def test_empty_header_only_input(self):
        write_input(self.root, [], raw_text="id,name,amount\n")
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual(out, [HEADER])
        self.assertIn("rows written=0", proc.stdout)

    def test_bad_header_is_hard_error_without_output(self):
        # All five contract header variants are hard errors: missing,
        # reordered, renamed, case-differing, and whitespace-padded.
        bad_headers = [
            "id,amount\n1,2\n",
            "name,id,amount\na,1,2\n",
            "id,fullname,amount\n1,a,1\n",
            "ID,Name,Amount\n1,a,1\n",
            " id,name,amount \n1,a,1\n",
        ]
        output_path = os.path.join(self.root, "data", "output.csv")
        write_input(self.root, [], raw_text=bad_headers[0])
        proc = self.run_pipeline()
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(os.path.exists(output_path))
        # A pre-existing output is left untouched, never partial.
        for bad_header in bad_headers:
            with self.subTest(header=bad_header):
                write_input(self.root, [("1", "a", "1")])
                self.assertEqual(self.run_pipeline().returncode, 0)
                before = output_bytes(self.root)
                write_input(self.root, [], raw_text=bad_header)
                proc = self.run_pipeline()
                self.assertNotEqual(proc.returncode, 0)
                self.assertEqual(output_bytes(self.root), before)

    def test_bom_prefixed_input_accepted(self):
        write_input(self.root, [], raw_text=(
            "\ufeffid,name,amount\n1,a,1\n2,b,2\n"))
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual(out, [
            HEADER,
            ["1", "a", "1.00"],
            ["2", "b", "2.00"],
        ])

    def test_output_bytes_lf_and_minimal_quoting(self):
        write_input(self.root, [
            ("1", "plain", "1"),
            ("2", "a,b", "2"),
            ("3", 'say "hi"', "3"),
        ])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        raw = output_bytes(self.root)
        self.assertNotIn(b"\r", raw)
        self.assertTrue(raw.endswith(b"\n"))
        self.assertEqual(raw, (
            b"id,name,amount\n"
            b"1,plain,1.00\n"
            b'2,"a,b",2.00\n'
            b'3,"say ""hi""",3.00\n'
        ))

    def test_missing_input_is_hard_error_without_output(self):
        proc = self.run_pipeline()
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(
            os.path.exists(os.path.join(self.root, "data", "output.csv")))

    def test_rerun_unchanged_is_byte_identical(self):
        write_input(self.root, [("2", " b ", "3.456"), ("1", "a", "1")])
        self.assertEqual(self.run_pipeline().returncode, 0)
        first = output_bytes(self.root)
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(output_bytes(self.root), first)
        self.assertIn("new rows=0", proc.stdout)

    def test_append_adds_exactly_new_row(self):
        write_input(self.root, [("1", "a", "1"), ("2", "b", "2")])
        self.assertEqual(self.run_pipeline().returncode, 0)
        with open(os.path.join(self.root, "data", "input.csv"),
                  "a", encoding="utf-8", newline="") as handle:
            handle.write("101,  newbie  ,9.999\n")
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual(len(out) - 1, 3)
        self.assertEqual(out[-1], ["101", "newbie", "10.00"])
        ids = [row[0] for row in out[1:]]
        self.assertEqual(len(set(ids)), len(ids))

    def test_modify_existing_row_reflected(self):
        write_input(self.root, [("1", "a", "1"), ("2", "b", "2")])
        self.assertEqual(self.run_pipeline().returncode, 0)
        write_input(self.root, [("1", "a", "1"), ("2", "changed", "7.5")])
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual(len(out) - 1, 2)
        self.assertEqual(out[2], ["2", "changed", "7.50"])

    def test_non_ascii_whitespace_is_not_whitespace(self):
        # U+00A0 is not in the contract's six-character ASCII set.
        write_input(self.root, [], raw_text=(
            "id,name,amount\n1,a,\u00a012.5\n2,b,3\n"))
        proc = self.run_pipeline()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        _, out = read_output(self.root)
        self.assertEqual([row[0] for row in out[1:]], ["2"])
        self.assertIn("rows skipped=1", proc.stdout)

    def test_internal_state_lives_outside_data(self):
        write_input(self.root, [("1", "a", "1")])
        self.assertEqual(self.run_pipeline().returncode, 0)
        data_contents = set(os.listdir(os.path.join(self.root, "data")))
        self.assertEqual(data_contents, {"input.csv", "output.csv"})
        self.assertTrue(
            os.path.exists(os.path.join(self.root, ".etl_state.json")))


if __name__ == "__main__":
    unittest.main()
