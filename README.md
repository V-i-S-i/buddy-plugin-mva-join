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
MVA JOIN <join_table>
    ON <main_table>.<mva_field> = <join_table>.<join_field>
    [AND <main_table>.<mva_field2> = <join_table>.<join_field> ...]
[WHERE <conditions>]
[GROUP BY <columns>]
[ORDER BY <columns>]
[LIMIT [offset,] count]
```

The `ON` clause must link the MVA field(s) on the main table to the corresponding field on the join table. Either side of `=` can be written first. Multiple `AND`-separated conditions are supported — an article must satisfy **all** conditions to match (INNER JOIN with AND semantics).

## Supported SELECT expressions

| Expression | Example | Notes |
|---|---|---|
| `*` | `SELECT *` | All columns from both tables |
| Join-table column | `categories.name` | Must be prefixed with the join table name |
| Main-table column | `articles.feed_id` | Must be prefixed with the main table name |
| `COUNT(*)` | `COUNT(*) AS cnt` | Counts matching main-table rows per join-table row |
| Aggregate functions | `SUM(articles.auditorium)` | `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT` — passed verbatim to Manticore after stripping the table prefix |
| `GROUP_CONCAT(DISTINCT col)` | `GROUP_CONCAT(DISTINCT articles.feed_id) AS feeds` | `DISTINCT` is stripped before sending to Manticore; deduplication is done in PHP |
| `GROUP_CONCAT(FUNC(...))` | `GROUP_CONCAT(SNIPPET(articles.content, QUERY(), 'around=40')) AS snips` | Nested function calls unsupported by Manticore are simulated in PHP: the inner expression is fetched per-category and the results are concatenated |
| MVA cross-field count | `SUM(articles.neutral_kw_id IN (categories.keyword_id))` | Counts articles where a second MVA field contains the current category's keyword |
| Arbitrary function | `SNIPPET(articles.content, QUERY()) AS snip` | Any function expression — `SNIPPET`, `WEIGHT()`, `GEODIST()`, etc. — is passed verbatim to the main-table query after stripping the table prefix. Join-table column references (e.g. `categories.category_name`) are substituted with the actual per-row value at runtime |
| String literal | `'label' AS col` | Fixed value repeated in every result row |

Column order in the result set matches the order in the SELECT list.

### Column name conflicts

When both tables have a column with the same bare name (e.g. both have `id`), the main-table column keeps the bare name and the join-table column is qualified as `{join_table}.{column}`:

```
articles_today_lt.id  →  id
categories.id         →  categories.id
```

Explicit `AS` aliases always take precedence and are never renamed.

## WHERE conditions

Conditions are routed automatically based on the table prefix:

- `categories.customer_id = 7037` → sent to the join-table query
- `articles.feed_id = 12345` → sent to the main-table query
- `customer_id = 7037` (no prefix) → assumed to be a join-table condition

Multiple conditions connected by `AND` (including across multiple lines) are split and routed independently. Conditions that reference both tables in a single `OR` expression are not supported.

## Execution modes

### Mode A — Aggregation (COUNT(\*) or GROUP BY present)

One result row per matched join-table row, with aggregate values. When `GROUP BY` references a **main-table column**, the result expands to one row per `(join-table row, group-value)` pair — useful for per-feed or per-source breakdowns within each category.

```sql
-- One row per category
SELECT categories.category_name, categories.id,
       COUNT(*) AS cnt,
       SUM(articles_today_lt.negative_keyword_id IN (categories.keyword_id)) AS cnt_negative,
       SUM(articles_today_lt.neutral_keyword_id  IN (categories.keyword_id)) AS cnt_neutral,
       SUM(articles_today_lt.positive_keyword_id IN (categories.keyword_id)) AS cnt_positive,
       GROUP_CONCAT(articles_today_lt.id) AS article_ids,
       GROUP_CONCAT(DISTINCT articles_today_lt.feed_id) AS distinct_feeds,
       GROUP_CONCAT(SNIPPET(articles_today_lt.content, QUERY(), 'around=40', 'limit=200')) AS snippets
FROM articles_today_lt
MVA JOIN categories
    ON articles_today_lt.keyword_id = categories.keyword_id
WHERE categories.customer_id = 7037
LIMIT 100;

-- One row per (category, feed) — GROUP BY main-table column
SELECT categories.id, categories.category_name,
       COUNT(*) AS cnt,
       SUM(articles_today_lt.negative_keyword_id IN (categories.keyword_id)) AS cnt_negative,
       MIN(articles_today_lt.date_added) AS min_date,
       articles_today_lt.feed_id
FROM articles_today_lt
MVA JOIN categories
    ON articles_today_lt.keyword_id = categories.keyword_id
WHERE categories.customer_id = 7037
GROUP BY articles_today_lt.feed_id
LIMIT 100;
```

**Multi-condition ON (AND):**
```sql
-- Only articles where BOTH keyword_id AND neutral_keyword_id match the category
MVA JOIN categories
    ON articles_today_lt.keyword_id = categories.keyword_id
    AND articles_today_lt.neutral_keyword_id = categories.keyword_id
```

Internally:
1. Fetches all join-table rows matching the WHERE.
2. Pre-filters keywords by running a quick `SELECT mvaField … GROUP BY mvaField` against the main table (eliminates SUM expressions for categories with zero matches under any main-table filters).
3. Runs one aggregation query with `SUM(mvaField IN (kw1, kw2, ...))` per join-table row to count matches.
4. For each matched join-table row, runs targeted per-category queries for aggregate expressions and raw main-table columns.

### Mode B — Row expansion (no GROUP BY)

One result row per `(main-table row, join-table row)` pair. Supports explicit column lists or `SELECT *`.

```sql
-- Explicit columns
SELECT articles.id, articles.title, categories.name
FROM articles
MVA JOIN categories ON articles.keyword_id = categories.keyword_id
WHERE categories.customer_id = 7037;

-- All columns from both tables
SELECT *
FROM articles
MVA JOIN categories ON articles.keyword_id = categories.keyword_id
WHERE categories.customer_id = 7037;

-- SNIPPET using the category name as the search term (substituted per row)
SELECT
    categories.id, categories.category_name,
    articles_today_lt.id AS article_id,
    SNIPPET(articles_today_lt.content, categories.category_name, 'around=40', 'limit=200', 'snippet_boundary=sentence') AS snip
FROM articles_today_lt
MVA JOIN categories ON articles_today_lt.keyword_id = categories.keyword_id
WHERE categories.customer_id = 7037 AND MATCH('keyword')
LIMIT 100;
```

Internally:
1. Fetches all join-table rows matching the WHERE.
2. Fetches matching main-table rows (capped at 50,000). Expressions that reference join-table columns are excluded from this bulk fetch.
3. For each matched category, runs one targeted query to evaluate the per-category expressions (e.g. SNIPPET with a per-category search term) for the relevant article IDs.
4. Expands each main-table row's MVA values into one output row per matching join-table row, merging per-category expression values.

## JOIN semantics

The plugin implements **INNER JOIN** semantics: join-table rows with zero matching main-table rows are excluded from the result.

## Limitations

- Join-table fetch is capped at **10,000 rows**. A warning is emitted via `trigger_error` when the cap is hit.
- Main-table fetch (Mode B) is capped at **50,000 rows**. A warning is emitted when the cap is hit.
- When no `LIMIT` is specified, Manticore returns at most **20 rows** by default. Add an explicit `LIMIT` to get more results.
- The `LIMIT` multiplier heuristic (`LIMIT n` → fetch `n × 100` main-table rows) may be insufficient when one main-table row expands into many join-table matches. If results appear truncated, increase the LIMIT or rely on the 50,000-row cap.
- Unqualified column names (without a `table.` prefix) in the SELECT list are ignored, as they are ambiguous.
- `OR` conditions that reference columns from both tables in a single clause cannot be automatically routed and are silently dropped.
- Aggregates on join-table columns (e.g. `SUM(categories.weight)`) are not supported — an error is returned.
- `GROUP_CONCAT` with nested function calls (e.g. `GROUP_CONCAT(SNIPPET(...))`) is simulated in PHP. One extra query per matched category is issued to collect the inner expression's values, which may be slow for large result sets.
- Multi-condition ON clause: all ON conditions must reference the **same join-table field** (`joinField`). Conditions with different join-table fields are not currently supported.
- **`HAVING` is not supported.** A `HAVING` clause is silently ignored and will produce incorrect results. *(TODO: detect and raise an error)*
- **Large join tables and aggregation query size.** In Mode A, one `SUM(mvaField IN (...))` expression is generated per matched join-table row. With hundreds of rows and many keywords, the resulting SQL string can grow very large. A pre-filter step reduces this in practice, but pathological cases may still hit Manticore's query-length limit. *(TODO: chunk into batches of N rows)*
- **Debug logging** is gated behind `--log-level=debug[v[v]]` — no disk I/O in normal production operation.

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
