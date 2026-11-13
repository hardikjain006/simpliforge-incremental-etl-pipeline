# Incremental ETL Pipeline

## 1. Purpose

A small command-line ETL pipeline that incrementally extracts rows from a
CSV input, applies one transformation and one validation rule, and loads
the surviving rows into a single CSV output. A JSON state file records
which rows have already been processed so repeated runs only handle new
rows. Every successful run prints one concise summary line to standard
output.

## 2. Command-line interface

The pipeline is invoked exactly as:

```
python3 pipeline.py --input INPUT --output OUTPUT --state STATE
```

- `--input`: path to the input CSV file (must exist and be readable).
- `--output`: path to the output CSV file (created if absent, appended to
  if present; see section 6).
- `--state`: path to the JSON state file (created if absent; see
  section 7).
- All three flags are required. A missing flag, an unknown flag, or an
  unreadable input file is a usage error (see section 9).

No other flags, subcommands, prompts, or interactive behavior exist.

## 3. Input format

- `INPUT` is a UTF-8 CSV file with a header row followed by data rows,
  parsed with the Python `csv` module default (`excel`) dialect, accepting
  `\n` and `\r\n` line breaks; quoted fields and embedded commas, quotes,
  and quoted line breaks follow that dialect.
- The header must be exactly `id,name,amount`, in that order.
- Each data row has three fields:
  - `id`: a non-empty string that uniquely identifies the row (an empty
    `id` makes the row invalid; see section 5).
  - `name`: an arbitrary string (may be empty).
  - `amount`: a decimal number in plain notation matching
    `^-?(0|[1-9][0-9]*)(\.[0-9]+)?$` with no surrounding whitespace, no
    `+` sign, no exponent, and no `NaN`/`Infinity` spellings (for example
    `12.5`, `-3`, `0.07`).
- The reference fixture contains 100 data rows. Implementations must
  handle any row count, including zero data rows (header only).
- Blank lines, defined as lines that are empty or contain only spaces or
  tabs (excluding the line break), are ignored and count toward neither
  `processed` nor any other summary counter.

## 4. Transformation (exactly one)

Every row that passes validation gains exactly one derived column:

- `amount_cents = int(round(amount * 100))`

using round-half-to-even on the exact decimal value (so `0.07` becomes
`7` and `12.5` becomes `1250`). No other transformation, enrichment,
filtering, reordering, or reformatting of passing rows is performed.

## 5. Validation rule (exactly one)

A row is valid if and only if it has exactly three fields, its `id` is
non-empty, and its `amount` field matches the section 3 grammar exactly
(no stripping) with numeric value greater than or equal to zero.

- Rows that violate this rule are rejected: they are excluded from the
  output, counted as `invalid` in the summary, and still recorded in the
  state file as processed (they are not retried on later runs). A row
  with fewer or more than three fields is invalid under this rule: it is
  counted as `invalid`, produces no output, and is recorded in state
  under its first field when that field is non-empty, otherwise it adds
  no id but still counts as `processed` and `invalid`. Example: with
  fresh state, input row `a,b` yields `processed=1 new=1 written=0
  invalid=1`.
- An empty `id` makes the row invalid: it is counted as `invalid`,
  produces no output, and is recorded in state under `""`. Apart from an
  empty `id`, `id` and `name` values never cause rejection on their own.
- Hence `1e3`, `" 12.5 "`, `NaN`, and `Infinity` are all invalid; valid
  rows reproduce `amount` verbatim, so surrounding spaces never appear in
  output because such rows are rejected.

## 6. Output format (exactly one output)

- `OUTPUT` is a UTF-8 CSV file with the header
  `id,name,amount,amount_cents`, in that order.
- Each run appends one row per newly processed valid input row, in input
  order, with `amount` reproduced verbatim from the input and
  `amount_cents` as defined in section 4.
- If `OUTPUT` does not exist it is created with the header row. If it
  exists, new rows are appended and the header is never duplicated.
- This contract covers only `OUTPUT` files that are absent or were produced
  by this pipeline (header exactly `id,name,amount,amount_cents`);
  behavior with any other pre-existing `OUTPUT` content is undefined and
  is not exercised.
- Invalid rows never appear in the output.
- The output file is only modified by successful runs (exit status 0).
  Failed runs leave it byte-for-byte unchanged.

## 7. State file and incremental semantics

- `STATE` is a UTF-8 JSON file holding a single object of the form
  `{"processed_ids": [...]}` where the list contains every input `id`
  the pipeline has finished processing, in first-seen order, including
  ids of invalid rows.
- A missing state file is treated as empty (`{"processed_ids": []}`)
  and is created by the run.
- A state file that is not valid JSON, or that lacks a
  `processed_ids` string list, is a runtime error (see section 9): the
  run fails and neither the output nor the state file is modified.
- Rows whose `id` is already in the state file, or appears twice within
  one input (the second and later occurrences), are skipped: they are
  counted in `processed` but in no other summary counter (not in `new`,
  `written`, or `invalid`), and produce no output row.
- On a successful run the state file is updated to include every newly
  processed id. A rerun against unchanged input and state therefore
  processes rows but writes nothing new.
- The state file is only modified by successful runs.

## 8. Run summary on standard output

Every successful run prints exactly one line to standard output, in this
exact format:

```
processed=N new=M written=W invalid=V
```

where each value is a non-negative integer:

- `processed`: data rows read from the input (excluding the header and
  blank lines).
- `new`: rows not already in the state file at run start, excluding
  within-run duplicates of an earlier row in the same input (the second
  and later occurrences of an id within one input do not count as `new`).
- `written`: rows appended to the output by this run.
- `invalid`: new rows rejected by the validation rule.

The invariant `written + invalid <= new <= processed` always holds.
Example: fresh state (`{"processed_ids": []}`) with input rows `a,x,1.00`
and `a,y,2.00` yields `processed=2 new=1 written=1 invalid=0`; the second
`a` row counts only in `processed`.
Nothing else is written to standard output on success. Diagnostics and
error messages go to standard error.

## 9. Exit codes and error behavior

- `0`: success. Output, state, and the summary line are produced as
  specified.
- `2`: usage error (missing/unknown flag, missing or unreadable input
  file, header other than `id,name,amount`). No output or state file is
  created or modified.
- `1`: runtime failure (corrupt state file, unwritable output or state
  path). The output and state files remain byte-for-byte unchanged.
- On any non-zero exit the program writes a human-readable message to
  standard error and writes nothing to standard output.

## 10. Determinism

Runs are deterministic: the same input file, output file content, and
state file content always produce the same resulting output file, state
file, summary line, and exit status, regardless of working directory or
run history. Row order in the output follows input order for every run.

## 11. Boundary cases

An implementation satisfies this contract only if it also handles:

1. Header-only input: prints `processed=0 new=0 written=0 invalid=0`,
   creates an output containing just the header if absent, and exits 0.
2. Fresh state (missing state file): processes all rows and creates the
   state file.
3. Unchanged rerun: with the same input and the state left by the
   previous run, prints `written=0 invalid=0` (with `new=0`) and leaves
   both files unchanged.
4. Extended input: rows appended to the input after a run are the only
   rows processed by the next run; previously written output rows are
   not duplicated.
5. All-invalid input: every row rejected, output gains no rows, summary
   shows `written=0` and `invalid` equal to the new row count.
6. Corrupt state file: non-zero exit with output and state untouched.
7. Missing input file: exit status 2 with no files created or modified.

## 12. Non-goals

Parallel execution, streaming progress reporting, configuration files,
logging frameworks, database backends, network access, scheduling, and
support for additional formats, transformations, validation rules, or
outputs are out of scope.
