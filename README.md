# buddy-plugin-mva-join

A [Manticore Buddy](https://github.com/manticoresoftware/manticoresearch-buddy) plugin that adds `MVA JOIN` syntax to Manticore Search — allowing you to JOIN a regular table on a Multi-Value Attribute (MVA) field, which Manticore does not support natively.

## How it works

Manticore does not support joining on MVA fields directly. This plugin intercepts queries that contain the `MVA JOIN` keyword and rewrites them into multiple standard Manticore queries, then assembles the result in PHP:

1. **Fetch join-table rows** matching the WHERE conditions on that table.
2. Collect all keyword values from the join field (an MVA field on the main table).
3. Execute one or more queries against the main table and combine the results.

## Query syntax

```sql
SELECT <columns>
FROM <main_table>
MVA JOIN <join_table> ON <main_table>.<mva_field> = <join_table>.<join_field>
[WHERE <conditions>]
[GROUP BY <columns>]
[ORDER BY <columns>]
[LIMIT [offset,] count]
```

The `ON` clause must link the MVA field on the main table to the corresponding field on the join table. Either side of `=` can be written first.

## Supported SELECT expressions

| Expression | Example | Notes |
|---|---|---|
| Join-table column | `categories.name` | Must be prefixed with the join table name |
| Main-table column | `articles.feed_id` | Must be prefixed with the main table name |
| `COUNT(*)` | `COUNT(*) AS cnt` | Counts matching main-table rows per join-table row |
| Aggregate functions | `SUM(articles.auditorium)` | `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT` supported |

Column order in the result set matches the order in the SELECT list.

## WHERE conditions

Conditions are routed automatically based on the table prefix:

- `categories.customer_id = 7037` → sent to the join-table query
- `articles.feed_id = 12345` → sent to the main-table query
- `customer_id = 7037` (no prefix) → assumed to be a join-table condition

Multiple conditions connected by `AND` (including across multiple lines) are split and routed independently. Conditions that reference both tables in a single `OR` expression are not supported.

## Execution modes

### Mode A — Aggregation (COUNT(\*) or GROUP BY present)

One result row per matched join-table row, with aggregate values.

```sql
SELECT categories.category_name, categories.id,
       COUNT(*),
       SUM(articles.auditorium),
       GROUP_CONCAT(articles.id)
FROM articles
MVA JOIN categories ON articles.keyword_id = categories.keyword_id
WHERE categories.customer_id = 7037
GROUP BY categories.id;
```

Internally:
1. Fetches all join-table rows matching the WHERE.
2. Runs one aggregation query with `SUM(mva_field IN (kw1, kw2, ...))` per join-table row to count matches.
3. For each matched join-table row, runs a targeted per-category query for any `SUM`/`GROUP_CONCAT`/etc. expressions and raw main-table columns.

### Mode B — Row expansion (no GROUP BY, main-table columns in SELECT)

One result row per `(main-table row, join-table row)` pair.

```sql
SELECT articles.id, articles.title, categories.name
FROM articles
MVA JOIN categories ON articles.keyword_id = categories.keyword_id
WHERE categories.customer_id = 7037;
```

Internally:
1. Fetches all join-table rows matching the WHERE.
2. Fetches matching main-table rows (capped at 50,000).
3. Expands each main-table row's MVA values into one output row per matching join-table row.

## JOIN semantics

The plugin implements **INNER JOIN** semantics: join-table rows with zero matching main-table rows are excluded from the result.

## Limitations

- Join-table fetch is capped at **10,000 rows**.
- Main-table fetch (Mode B) is capped at **50,000 rows**.
- Unqualified column names (without a `table.` prefix) in the SELECT list are ignored, as they are ambiguous.
- `OR` conditions that reference columns from both tables in a single clause cannot be automatically routed and are silently dropped.
- Aggregates on join-table columns (e.g. `SUM(categories.weight)`) are not supported.

## Installation

```sql
CREATE PLUGIN visi/buddy-plugin-mva-join TYPE 'buddy' VERSION 'dev-main'
```

> **Note:** The statement must be on a single line with no trailing semicolon when sent via the HTTP API. The MySQL client adds a semicolon automatically.

## Requirements

- PHP 8.1+
- `manticoresoftware/buddy-core` ^0.1 | ^1.0 | ^2.0 | ^3.0

## Debug logs

The plugin writes diagnostic logs to:

- `/tmp/mva-join-debug.log` — query detection (`hasMatch`)
- `/tmp/mva-join-handler.log` — full execution trace including all sub-queries and row counts
