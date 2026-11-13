"""Public black-box tests for the Incremental ETL pipeline CLI.

Every test invokes the product only through ``python3 pipeline.py`` in a
subprocess. No product module is imported and no private symbol is pinned.
"""

import csv
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

PIPELINE = pathlib.Path(__file__).resolve().parent.parent / "pipeline.py"
SUMMARY_RE = re.compile(r"^processed=(\d+) new=(\d+) written=(\d+) invalid=(\d+)\n$")


def run_pipeline(input_path, output_path, state_path, extra_args=None):
    cmd = [sys.executable, str(PIPELINE)]
    if extra_args is not None:
        cmd.extend(extra_args)
    else:
        cmd.extend(
            ["--input", input_path, "--output", output_path, "--state", state_path]
        )
    return subprocess.run(cmd, capture_output=True, text=True, timeout=60)


def write_text(path, text):
    pathlib.Path(path).write_text(text, encoding="utf-8")


def read_text(path):
    return pathlib.Path(path).read_text(encoding="utf-8")


def read_output_rows(output_path):
    with open(output_path, "r", encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def read_state_ids(state_path):
    with open(state_path, "r", encoding="utf-8") as f:
        return json.load(f)["processed_ids"]


def parse_summary(proc):
    assert proc.stdout is not None
    m = SUMMARY_RE.match(proc.stdout)
    assert m is not None, f"bad summary: {proc.stdout!r}"
    return tuple(int(g) for g in m.groups())


class PipelineBlackBoxTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.input_path = os.path.join(self.tmp.name, "in.csv")
        self.output_path = os.path.join(self.tmp.name, "out.csv")
        self.state_path = os.path.join(self.tmp.name, "state.json")

    # -- helpers ------------------------------------------------------
    def run_ok(self, input_text, state_text=None):
        write_text(self.input_path, input_text)
        if state_text is not None:
            write_text(self.state_path, state_text)
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 0, msg=f"stderr: {proc.stderr}")
        self.assertEqual(proc.stderr, "")
        return proc

    # -- section 2: CLI ----------------------------------------------
    def test_missing_flag_is_usage_error(self):
        write_text(self.input_path, "id,name,amount\n")
        proc = subprocess.run(
            [sys.executable, str(PIPELINE), "--input", self.input_path],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(proc.stdout, "")
        self.assertNotEqual(proc.stderr, "")

    def test_unknown_flag_is_usage_error(self):
        write_text(self.input_path, "id,name,amount\n")
        proc = run_pipeline(
            self.input_path,
            self.output_path,
            self.state_path,
            extra_args=[
                "--input",
                self.input_path,
                "--output",
                self.output_path,
                "--state",
                self.state_path,
                "--extra",
                "1",
            ],
        )
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(proc.stdout, "")
        self.assertNotEqual(proc.stderr, "")

    def test_missing_input_is_usage_error_with_no_files_created(self):
        missing = os.path.join(self.tmp.name, "does-not-exist.csv")
        proc = run_pipeline(missing, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(proc.stdout, "")
        self.assertNotEqual(proc.stderr, "")
        self.assertFalse(os.path.exists(self.output_path))
        self.assertFalse(os.path.exists(self.state_path))

    def test_input_directory_is_usage_error(self):
        proc = run_pipeline(self.tmp.name, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(proc.stdout, "")

    def test_bad_header_is_usage_error_and_leaves_files_untouched(self):
        write_text(self.input_path, "id,wrong,amount\na,x,1.00\n")
        write_text(self.output_path, "id,name,amount,amount_cents\n")
        write_text(self.state_path, '{"processed_ids": []}\n')
        before_out = read_text(self.output_path)
        before_state = read_text(self.state_path)
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(proc.stdout, "")
        self.assertNotEqual(proc.stderr, "")
        self.assertEqual(read_text(self.output_path), before_out)
        self.assertEqual(read_text(self.state_path), before_state)

    # -- section 11.1: header only ------------------------------------
    def test_header_only_creates_output_with_header(self):
        proc = self.run_ok("id,name,amount\n")
        self.assertEqual(parse_summary(proc), (0, 0, 0, 0))
        self.assertEqual(
            read_output_rows(self.output_path),
            [["id", "name", "amount", "amount_cents"]],
        )
        self.assertEqual(read_state_ids(self.state_path), [])

    # -- basic fresh run ----------------------------------------------
    def test_fresh_run_writes_valid_rows_verbatim(self):
        proc = self.run_ok("id,name,amount\na,x,1.00\nb,y,2.50\n")
        self.assertEqual(parse_summary(proc), (2, 2, 2, 0))
        self.assertEqual(
            read_output_rows(self.output_path),
            [
                ["id", "name", "amount", "amount_cents"],
                ["a", "x", "1.00", "100"],
                ["b", "y", "2.50", "250"],
            ],
        )
        self.assertEqual(read_state_ids(self.state_path), ["a", "b"])

    def test_transformation_amount_cents(self):
        cases = [
            ("0.07", "7"),
            ("12.5", "1250"),
            ("1.00", "100"),
            ("0", "0"),
            ("0.00", "0"),
            ("2.675", "268"),
            ("1.005", "100"),
            ("1.015", "102"),
            ("0.005", "0"),
            ("0.015", "2"),
        ]
        for amount, expected in cases:
            with self.subTest(amount=amount):
                tmp_in = os.path.join(self.tmp.name, f"in-{amount}.csv")
                tmp_out = os.path.join(self.tmp.name, f"out-{amount}.csv")
                tmp_state = os.path.join(self.tmp.name, f"state-{amount}.json")
                write_text(tmp_in, f"id,name,amount\nk,n,{amount}\n")
                proc = run_pipeline(tmp_in, tmp_out, tmp_state)
                self.assertEqual(proc.returncode, 0, msg=proc.stderr)
                rows = read_output_rows(tmp_out)
                self.assertEqual(rows[1], ["k", "n", amount, expected])

    def test_output_preserves_amount_verbatim(self):
        proc = self.run_ok("id,name,amount\na,n,1.00\nb,n,2.50\n")
        self.assertEqual(proc.returncode, 0)
        rows = read_output_rows(self.output_path)
        self.assertEqual(rows[1][2], "1.00")
        self.assertEqual(rows[2][2], "2.50")

    # -- section 5: validation -----------------------------------------
    def test_invalid_amounts_are_rejected_but_recorded(self):
        bad = [
            "1e3",
            " 12.5 ",
            "NaN",
            "Infinity",
            "+12",
            "01",
            ".5",
            "5.",
            "",
            "12.5.6",
            "--3",
            "-3",
            "-1.5",
            "abc",
        ]
        for amount in bad:
            with self.subTest(amount=amount):
                tmp_in = os.path.join(self.tmp.name, "in-bad.csv")
                tmp_out = os.path.join(self.tmp.name, "out-bad.csv")
                tmp_state = os.path.join(self.tmp.name, "state-bad.json")
                for p in (tmp_in, tmp_out, tmp_state):
                    if os.path.exists(p):
                        os.unlink(p)
                write_text(tmp_in, f"id,name,amount\nk,n,{amount}\n")
                proc = run_pipeline(tmp_in, tmp_out, tmp_state)
                self.assertEqual(proc.returncode, 0, msg=proc.stderr)
                self.assertEqual(parse_summary(proc), (1, 1, 0, 1))
                self.assertEqual(
                    read_output_rows(tmp_out),
                    [["id", "name", "amount", "amount_cents"]],
                )
                self.assertEqual(read_state_ids(tmp_state), ["k"])

    def test_negative_zero_is_valid(self):
        for amount in ("-0", "-0.00"):
            with self.subTest(amount=amount):
                tmp_in = os.path.join(self.tmp.name, "in-negzero.csv")
                tmp_out = os.path.join(self.tmp.name, "out-negzero.csv")
                tmp_state = os.path.join(self.tmp.name, "state-negzero.json")
                for p in (tmp_in, tmp_out, tmp_state):
                    if os.path.exists(p):
                        os.unlink(p)
                write_text(tmp_in, f"id,name,amount\nk,n,{amount}\n")
                proc = run_pipeline(tmp_in, tmp_out, tmp_state)
                self.assertEqual(proc.returncode, 0, msg=proc.stderr)
                self.assertEqual(parse_summary(proc), (1, 1, 1, 0))
                rows = read_output_rows(tmp_out)
                self.assertEqual(rows[1][:3], ["k", "n", amount])
                self.assertEqual(rows[1][3], "0")

    def test_empty_id_is_invalid_and_recorded_under_empty_string(self):
        proc = self.run_ok("id,name,amount\n,x,1.00\n")
        self.assertEqual(parse_summary(proc), (1, 1, 0, 1))
        self.assertEqual(
            read_output_rows(self.output_path),
            [["id", "name", "amount", "amount_cents"]],
        )
        self.assertEqual(read_state_ids(self.state_path), [""])
        # Second empty id in the next run is a duplicate: skipped.
        write_text(self.input_path, "id,name,amount\n,x,1.00\n")
        proc2 = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc2.returncode, 0)
        self.assertEqual(parse_summary(proc2), (1, 0, 0, 0))

    def test_short_row_is_invalid_and_recorded_under_first_field(self):
        proc = self.run_ok("id,name,amount\na,b\n")
        self.assertEqual(parse_summary(proc), (1, 1, 0, 1))
        self.assertEqual(
            read_output_rows(self.output_path),
            [["id", "name", "amount", "amount_cents"]],
        )
        self.assertEqual(read_state_ids(self.state_path), ["a"])

    def test_long_row_is_invalid_and_recorded_under_first_field(self):
        proc = self.run_ok("id,name,amount\na,b,c,d\n")
        self.assertEqual(parse_summary(proc), (1, 1, 0, 1))
        self.assertEqual(read_state_ids(self.state_path), ["a"])

    def test_short_row_with_empty_first_field_adds_no_id(self):
        proc = self.run_ok("id,name,amount\n,b\n")
        self.assertEqual(parse_summary(proc), (1, 1, 0, 1))
        self.assertEqual(read_state_ids(self.state_path), [])

    def test_single_field_row_is_invalid(self):
        proc = self.run_ok("id,name,amount\nlonely\n")
        self.assertEqual(parse_summary(proc), (1, 1, 0, 1))
        self.assertEqual(read_state_ids(self.state_path), ["lonely"])

    def test_duplicate_id_within_run_counts_once(self):
        proc = self.run_ok("id,name,amount\na,x,1.00\na,y,2.00\n")
        self.assertEqual(parse_summary(proc), (2, 1, 1, 0))
        self.assertEqual(
            read_output_rows(self.output_path),
            [
                ["id", "name", "amount", "amount_cents"],
                ["a", "x", "1.00", "100"],
            ],
        )
        self.assertEqual(read_state_ids(self.state_path), ["a"])

    def test_duplicate_invalid_id_within_run_counts_once(self):
        proc = self.run_ok("id,name,amount\na,x,-1\na,y,-1\n")
        self.assertEqual(parse_summary(proc), (2, 1, 0, 1))
        self.assertEqual(
            read_output_rows(self.output_path),
            [["id", "name", "amount", "amount_cents"]],
        )
        self.assertEqual(read_state_ids(self.state_path), ["a"])

    def test_id_and_name_values_never_reject_on_their_own(self):
        proc = self.run_ok("id,name,amount\n a b!@#,x y !,1.00\n")
        self.assertEqual(parse_summary(proc), (1, 1, 1, 0))

    def test_blank_lines_are_ignored(self):
        proc = self.run_ok(
            "id,name,amount\n"
            "\n"
            "a,x,1.00\n"
            "   \n"
            "\t\n"
            " \t \n"
            "b,y,2.00\n"
            "\n"
        )
        self.assertEqual(parse_summary(proc), (2, 2, 2, 0))
        self.assertEqual(len(read_output_rows(self.output_path)), 3)

    def test_quoted_fields_and_embedded_commas(self):
        write_text(
            self.input_path,
            'id,name,amount\n'
            'a,"x, y",1.00\n'
            'b,"q""q",2.00\n',
        )
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertEqual(parse_summary(proc), (2, 2, 2, 0))
        rows = read_output_rows(self.output_path)
        self.assertEqual(rows[1], ["a", "x, y", "1.00", "100"])
        self.assertEqual(rows[2], ["b", 'q"q', "2.00", "200"])

    def test_crlf_line_breaks_are_accepted(self):
        with open(self.input_path, "wb") as f:
            f.write(b"id,name,amount\r\na,x,1.00\r\nb,y,2.00\r\n")
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertEqual(parse_summary(proc), (2, 2, 2, 0))

    # -- sections 6-7: output/state and incremental semantics ----------
    def test_unchanged_rerun_writes_nothing(self):
        self.run_ok("id,name,amount\na,x,1.00\nb,y,2.00\n")
        before_out = read_text(self.output_path)
        before_state = read_text(self.state_path)
        write_text(self.input_path, "id,name,amount\na,x,1.00\nb,y,2.00\n")
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(parse_summary(proc), (2, 0, 0, 0))
        self.assertEqual(read_text(self.output_path), before_out)
        self.assertEqual(read_text(self.state_path), before_state)

    def test_extended_input_appends_only_new_rows(self):
        self.run_ok("id,name,amount\na,x,1.00\n")
        write_text(self.input_path, "id,name,amount\na,x,1.00\nb,y,2.00\n")
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(parse_summary(proc), (2, 1, 1, 0))
        self.assertEqual(
            read_output_rows(self.output_path),
            [
                ["id", "name", "amount", "amount_cents"],
                ["a", "x", "1.00", "100"],
                ["b", "y", "2.00", "200"],
            ],
        )
        self.assertEqual(read_state_ids(self.state_path), ["a", "b"])

    def test_output_header_is_never_duplicated(self):
        self.run_ok("id,name,amount\na,x,1.00\n")
        write_text(self.input_path, "id,name,amount\nb,y,2.00\n")
        run_pipeline(self.input_path, self.output_path, self.state_path)
        rows = read_output_rows(self.output_path)
        headers = [r for r in rows if r == ["id", "name", "amount", "amount_cents"]]
        self.assertEqual(len(headers), 1)

    def test_all_invalid_input_writes_no_rows(self):
        proc = self.run_ok("id,name,amount\na,x,-1\nb,y,1e3\n,x,1.00\n")
        self.assertEqual(parse_summary(proc), (3, 3, 0, 3))
        self.assertEqual(
            read_output_rows(self.output_path),
            [["id", "name", "amount", "amount_cents"]],
        )
        self.assertEqual(read_state_ids(self.state_path), ["a", "b", ""])

    def test_invalid_rows_are_not_retried(self):
        self.run_ok("id,name,amount\na,x,-5\nb,y,1.00\n")
        write_text(self.input_path, "id,name,amount\na,x,-5\nb,y,1.00\nc,z,3.00\n")
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(parse_summary(proc), (3, 1, 1, 0))
        rows = read_output_rows(self.output_path)
        self.assertEqual([r[0] for r in rows[1:]], ["b", "c"])

    def test_state_keeps_first_seen_order_including_invalid(self):
        proc = self.run_ok("id,name,amount\nb,y,1.00\na,x,-1\nc,z,2.00\n")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(read_state_ids(self.state_path), ["b", "a", "c"])

    def test_fresh_state_file_is_created(self):
        self.assertFalse(os.path.exists(self.state_path))
        self.run_ok("id,name,amount\na,x,1.00\n")
        self.assertTrue(os.path.exists(self.state_path))
        self.assertEqual(read_state_ids(self.state_path), ["a"])

    def test_summary_invariant_holds(self):
        proc = self.run_ok(
            "id,name,amount\na,x,1.00\na,y,2.00\nb,z,-1\nc,w,3.00\n"
        )
        processed, new, written, invalid = parse_summary(proc)
        self.assertEqual((processed, new, written, invalid), (4, 3, 2, 1))
        self.assertLessEqual(written + invalid, new)
        self.assertLessEqual(new, processed)

    def test_success_stdout_is_exactly_one_line(self):
        proc = self.run_ok("id,name,amount\na,x,1.00\n")
        self.assertEqual(proc.returncode, 0)
        self.assertRegex(proc.stdout, r"^processed=\d+ new=\d+ written=\d+ invalid=\d+\n$")
        self.assertEqual(proc.stdout.count("\n"), 1)

    def test_deterministic_across_working_directories(self):
        self.run_ok("id,name,amount\na,x,1.00\nb,y,2.00\n")
        first_out = read_text(self.output_path)
        first_state = read_text(self.state_path)
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        proc = subprocess.run(
            [sys.executable, str(PIPELINE),
             "--input", self.input_path,
             "--output", self.output_path,
             "--state", self.state_path],
            capture_output=True,
            text=True,
            cwd=other.name,
            timeout=60,
        )
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(parse_summary(proc), (2, 0, 0, 0))
        self.assertEqual(read_text(self.output_path), first_out)
        self.assertEqual(read_text(self.state_path), first_state)

    def test_hundred_row_fixture(self):
        lines = ["id,name,amount"]
        for i in range(100):
            lines.append(f"r{i},name-{i},{i}.00")
        proc = self.run_ok("\n".join(lines) + "\n")
        self.assertEqual(parse_summary(proc), (100, 100, 100, 0))
        rows = read_output_rows(self.output_path)
        self.assertEqual(len(rows), 101)
        self.assertEqual(rows[1], ["r0", "name-0", "0.00", "0"])
        self.assertEqual(rows[50], ["r49", "name-49", "49.00", "4900"])
        self.assertEqual(len(read_state_ids(self.state_path)), 100)

    # -- section 9: corrupt state / unwritable paths -------------------
    def test_corrupt_state_non_json_leaves_files_untouched(self):
        self.run_ok("id,name,amount\na,x,1.00\n")
        before_out = read_text(self.output_path)
        write_text(self.state_path, "{not json")
        before_state = read_text(self.state_path)
        write_text(self.input_path, "id,name,amount\nb,y,2.00\n")
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stdout, "")
        self.assertNotEqual(proc.stderr, "")
        self.assertEqual(read_text(self.output_path), before_out)
        self.assertEqual(read_text(self.state_path), before_state)

    def test_corrupt_state_bad_shape_leaves_files_untouched(self):
        bad_states = [
            '{"other": []}\n',
            '{"processed_ids": "a"}\n',
            '{"processed_ids": [1, 2]}\n',
            '[1, 2, 3]\n',
            '[]\n',
            '"hello"\n',
        ]
        for bad in bad_states:
            with self.subTest(state=bad):
                tmp_in = os.path.join(self.tmp.name, "in-cs.csv")
                tmp_out = os.path.join(self.tmp.name, "out-cs.csv")
                tmp_state = os.path.join(self.tmp.name, "state-cs.json")
                write_text(tmp_in, "id,name,amount\na,x,1.00\n")
                write_text(tmp_out, "id,name,amount,amount_cents\n")
                before_out = read_text(tmp_out)
                write_text(tmp_state, bad)
                before_state = read_text(tmp_state)
                proc = run_pipeline(tmp_in, tmp_out, tmp_state)
                self.assertEqual(proc.returncode, 1, msg=f"state={bad!r}")
                self.assertEqual(proc.stdout, "")
                self.assertNotEqual(proc.stderr, "")
                self.assertEqual(read_text(tmp_out), before_out)
                self.assertEqual(read_text(tmp_state), before_state)

    def test_failed_runs_write_nothing_to_stdout(self):
        write_text(self.input_path, "id,name,amount\n")
        write_text(self.state_path, "bad json")
        proc = run_pipeline(self.input_path, self.output_path, self.state_path)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "")

    def test_output_path_directory_is_runtime_error(self):
        write_text(self.input_path, "id,name,amount\na,x,1.00\n")
        write_text(self.state_path, '{"processed_ids": []}\n')
        before_state = read_text(self.state_path)
        proc = run_pipeline(self.input_path, self.tmp.name, self.state_path)
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(read_text(self.state_path), before_state)

    def test_state_path_directory_is_runtime_error(self):
        write_text(self.input_path, "id,name,amount\na,x,1.00\n")
        proc = run_pipeline(self.input_path, self.output_path, self.tmp.name)
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stdout, "")
        self.assertFalse(os.path.exists(self.output_path))


if __name__ == "__main__":
    unittest.main()

