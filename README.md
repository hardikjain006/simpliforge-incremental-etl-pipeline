# Incremental ETL Pipeline

## Goal

Provide an incremental ETL pipeline over a 100-row fixture with exactly
one transformation, one validation rule, and one output.

## Scope

- In scope: reading the 100-row fixture, applying the single
  transformation, enforcing the single validation rule, writing the
  single output, and incremental rerun behavior.
- Non-goals: additional transformations, additional validation rules,
  additional outputs or reports, streaming, scheduling, and external
  services or databases.

## Inputs

### Source fixture

- Path: `data/input.csv` (relative to the repository root).
- Encoding: UTF-8, LF line endings, with a single header row.
- The fixture contains exactly 100 data rows.
- Header columns, in order: `id,name,amount`.
- Column definitions:
  - `id`: decimal integer, unique across the file, values 1 through 100
    in the reference fixture. Within the test contract, every `id`
    field, after trimming surrounding ASCII whitespace, matches an
    optional `+` or `-` sign followed by one or more ASCII digits. An
    `id` field that does not match this form (for example `abc`,
    `1.5`, or an empty field) is outside the test contract and carries
    no testable verdict; tests use only integer `id` fields.
  - `name`: arbitrary non-empty display string; surrounding or
    repeated internal whitespace is permitted in the input.
  - `amount`: decimal number in plain notation (for example `12.50`),
    using the lexical grammar defined under the validation rule below.
- "ASCII whitespace" everywhere in this contract means exactly the six
  characters HT (`0x09`), LF (`0x0A`), VT (`0x0B`), FF (`0x0C`),
  CR (`0x0D`), and space (`0x20`). No other character is treated as
  whitespace for trimming or collapsing.
- A single leading UTF-8 byte-order mark, when present, is removed
  before header comparison. Otherwise the header row must match exactly
  `id,name,amount` in order: any missing, reordered, renamed,
  case-differing, or whitespace-padded header is a hard error — the
  pipeline reports the problem and exits non-zero without writing a
  partial output.
- A missing or unreadable input file is a hard error: the pipeline
  reports the problem and exits non-zero without writing a partial
  output.

## Transformation (exactly one)

- The pipeline applies exactly one transformation: `name`
  normalization.
- Normalization is defined as: strip leading and trailing ASCII
  whitespace (the set defined under Inputs) from `name` and collapse
  every interior run of ASCII whitespace to a single space (`0x20`).
  Letter case is preserved.
- The transformation is pure and deterministic: the same input row
  always yields the same normalized row.
- No other transformation is applied. In particular, `id` values and
  numeric `amount` values pass through unchanged apart from the
  output formatting defined below.

## Validation rule (exactly one)

- The pipeline enforces exactly one validation rule: `amount` must be
  a finite decimal number greater than or equal to zero.
- The accepted `amount` grammar, applied after trimming surrounding
  ASCII whitespace (the set defined under Inputs), is: an optional `+`
  or `-` sign, followed by one or more ASCII digits, optionally followed
  by a single `.` and one or more ASCII digits. Anything else —
  including scientific notation (`1e3`), `Infinity`/`NaN` in any casing,
  empty or whitespace-only fields, `.5`, and `5.` — is invalid. Signed
  zero (`-0`, `-0.00`) is valid because its numeric value equals zero;
  any other negative value is invalid.
- A row failing this rule (missing, non-numeric, infinite, or
  negative `amount`) is invalid.
- Invalid rows are skipped: they do not appear in the output.
- Skipped rows are reported through the run summary (counts), not
  through an additional output file.
- No other row-level rejection is permitted within the test contract.
  Rows with duplicate or out-of-range `id` values and rows with unusual `name` content are
  still processed as long as they satisfy the `amount` rule; such
  fixture defects are outside this contract. Rows whose `id` field is
  not an integer in the sense defined under Inputs are outside the
  test contract and carry no testable verdict.

## Output (exactly one)

- Path: `data/output.csv` (relative to the repository root).
- Encoding: UTF-8, LF line endings, with a single header row.
- Header columns, in order: `id,name,amount`.
- Content: one row per valid input row, with `name` normalized per
  the transformation and `amount` formatted with exactly two decimal
  places (for example `7` becomes `7.00`). Values with more than two
  decimal places are rounded half away from zero to two decimals (for
  example `12.345` becomes `12.35` and `2.675` becomes `2.68`). A valid `amount` whose numeric value is zero -- including signed zero (`-0`, `-0.00`) -- always renders as `0.00`, never `-0.00`.
- `id` renders in canonical form: the integer value in decimal with no
  leading zeros and no surrounding whitespace (input ` 007 ` renders
  as `7`).
- Output CSV field quoting follows minimal RFC-4180 style: a field is
  enclosed in double quotes (`"`) if and only if it contains a comma
  (`,`), a double quote (`"`), or a line break (`CR`/`LF`); an
  embedded double quote is escaped by doubling it (`""`); no
  backslash escapes are used and no other field is quoted. Because
  `name` normalization collapses every interior run of
  ASCII whitespace (including `CR` and `LF`) to a single space and
  strips leading and trailing ASCII whitespace, a normalized `name`
  never contains a line break, so quoting in practice triggers only on
  commas and double quotes (for example `a,b` renders as `"a,b"` and
  `say "hi"` renders as `"say ""hi"""`). Input rows are parsed with
  the same quoting convention, so such names can be expressed in the
  fixture.
- Row order: ascending numeric `id`. Rows sharing a duplicate `id` keep
  their relative input order (stable within the equal-`id` group).
- The output file is fully rewritten on each successful run so that
  it always reflects the current input; it never contains duplicate
  rows from repeated runs.
- No second output, report, or export file is part of this contract.
  A run summary (rows read, rows written, rows skipped) is emitted to
  standard output or standard error as a human-readable message and
  is not a file output.

## Incremental behavior

- The implementation must document one command that runs the pipeline
  end to end from `data/input.csv` to `data/output.csv`.
- Reruns must be incremental in observable effect:
  - A rerun with an unchanged input reproduces a byte-identical
    `data/output.csv` and processes no rows as new.
  - A rerun after appending new input rows adds exactly the new
    valid rows to the output without duplicating existing rows.
  - A rerun after modifying an existing input row reflects the
    modified row in the output.
- How incrementality is achieved internally (for example a local
  checkpoint or state file) is an implementation detail, provided
  the single-output contract above holds and reruns remain
  deterministic. Any internal state must live outside `data/`
  or be documented alongside the run command.

## Determinism

- The same input file always produces a byte-identical output file,
  including column order, row order, number formatting, and line
  endings.
- The pipeline performs no network access and seeds any randomness;
  in practice the specified transformation and validation require no
  randomness at all.

## Boundary cases

- Empty input (header only): succeeds and writes a header-only
  output with a summary of zero rows written.
- Input row with `amount` of `0`, `0.00`, `-0`, or whitespace-padded
  numerics: `0` (including signed zero) is valid; surrounding ASCII
  whitespace around `amount` is trimmed before validation; signed zero renders as `0.00` in the output.
- Input row with `amount` of `12.345`: valid; the output carries `12.35`
  per the half-away-from-zero rounding rule.
- Input row with `amount` in scientific notation (`1e3`) or spelled
  `Infinity`/`NaN`: invalid, skipped, and counted as skipped.
- Input with a missing, reordered, renamed, case-differing, or
  whitespace-padded header row: hard error, non-zero exit, no partial
  output.
- Input rows sharing a duplicate `id`: every row satisfying the `amount`
  rule is processed; `id` renders canonically and equal-`id` rows keep
  input order.
- Input row with empty `name` after normalization: still processed
  (there is no name validation rule); the output carries the empty
  string for that row.
- Input row with a `name` containing a comma or double quote (for
  example `a,b` or `say "hi"`): still processed as long as it
  satisfies the `amount` rule; the output field is quoted per the
  quoting rule above.
- Input row with a non-integer `id` field (for example `abc`, `1.5`,
  or an empty field): outside the test contract with no testable
  verdict; tests use only integer `id` fields as defined under
  Inputs.
- Malformed row (wrong column count or unparseable CSV): treated as
  failing the validation rule, skipped, and counted as skipped.
- Missing input file: hard error, non-zero exit, no partial output.

## Acceptance criteria

1. With the 100-row reference fixture where every row is valid, one
   run writes `data/output.csv` with 100 data rows ordered by
   ascending `id`, normalized `name` values, and two-decimal
   `amount` values.
2. With a fixture containing at least one negative or non-numeric
   `amount`, one run writes only the valid rows, omits the invalid
   rows, and reports the skipped count.
3. A second run with an unchanged input leaves `data/output.csv`
   byte-identical.
4. Appending a new valid row (for example `id` 101) and rerunning
   adds exactly that row to the output with no duplicates.
5. The repository documents the single run command and the locations
   of the input and output files defined above.

## Open questions

- None. The contract above is sufficient to implement and test
  against; any residual fixture-content choice (exact names and
  amounts in the 100 rows) is left to the implementation phase.
