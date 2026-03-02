<?php declare(strict_types=1);

/*
 * Manticore Buddy Plugin: MVA JOIN
 * Simulates JOIN on Multi-Value Attribute fields by decomposing the query
 * into a join-table fetch + PHP-side aggregation/expansion.
 */

namespace Manticoresearch\Buddy\Plugin\MvaJoin;

use Manticoresearch\Buddy\Core\ManticoreSearch\Client as HTTPClient;
use Manticoresearch\Buddy\Core\Plugin\BaseHandlerWithClient;
use Manticoresearch\Buddy\Core\Task\Column;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;

/**
 * Resolves MVA JOIN queries in two passes:
 *
 * MODE A – Aggregation (COUNT(*) / GROUP BY present):
 *   1. Fetch join table rows matching WHERE conditions.
 *   2. Build one SUM(mvaField IN (kw1,kw2,...)) expression per join-table row.
 *   3. Execute a single aggregation query against the main table.
 *   4. Map the per-category counts back into a result set.
 *
 * MODE B – Row query (main table fields in SELECT, no GROUP BY):
 *   1. Fetch join table rows matching WHERE conditions.
 *   2. Collect all unique keyword values.
 *   3. Fetch matching main table rows (with pagination guard).
 *   4. For each main table row, expand its MVA values to one result
 *      row per matching join-table row (deduplicated per join row).
 */
final class Handler extends BaseHandlerWithClient
{
	public function __construct(public Payload $payload)
	{
	}

	public function run(): Task
	{
		// -----------------------------------------------------------------------
		// All logic lives inside this static closure so it can be safely passed
		// to Task::create() without capturing $this.
		// -----------------------------------------------------------------------
		$taskFn = static function (
			Payload $payload,
			HTTPClient $manticoreClient
		): TaskResult {

			$logFile = '/tmp/mva-join-handler.log';
			file_put_contents($logFile, "\n[" . date('Y-m-d H:i:s') . "] Handler started\n", FILE_APPEND);

			$query = $payload->query;
			file_put_contents($logFile, '  Original query: ' . substr($query, 0, 400) . "\n", FILE_APPEND);

			// ==================================================================
			// HELPER CLOSURES
			// ==================================================================

			/**
			 * Split a comma-separated list respecting nested parentheses.
			 * Used for SELECT list and GROUP BY list.
			 */
			$splitByComma = static function (string $str): array {
				$parts = [];
				$depth = 0;
				$cur = '';
				for ($i = 0, $len = strlen($str); $i < $len; $i++) {
					$ch = $str[$i];
					if ($ch === '(') {
						$depth++;
						$cur .= $ch;
					} elseif ($ch === ')') {
						$depth--;
						$cur .= $ch;
					} elseif ($ch === ',' && $depth === 0) {
						$parts[] = trim($cur);
						$cur = '';
					} else {
						$cur .= $ch;
					}
				}
				if (trim($cur) !== '') {
					$parts[] = trim($cur);
				}
				return $parts;
			};

			/**
			 * Split WHERE clause by top-level AND (not inside parentheses).
			 */
			$splitWhereByAnd = static function (string $where): array {
				$parts = [];
				$depth = 0;
				$cur = '';
				$i = 0;
				$len = strlen($where);
				while ($i < $len) {
					$ch = $where[$i];
					if ($ch === '(') {
						$depth++;
						$cur .= $ch;
						$i++;
					} elseif ($ch === ')') {
						$depth--;
						$cur .= $ch;
						$i++;
					} elseif ($depth === 0 && strtoupper(substr($where, $i, 5)) === ' AND ') {
						if (trim($cur) !== '') {
							$parts[] = trim($cur);
						}
						$cur = '';
						$i += 5;
					} else {
						$cur .= $ch;
						$i++;
					}
				}
				if (trim($cur) !== '') {
					$parts[] = trim($cur);
				}
				return $parts;
			};

			/**
			 * Extract MVA values from a field value.
			 * Manticore returns MVA as a comma-separated string like "38216,38218,38219".
			 * Returns an array of trimmed non-empty string values.
			 */
			$extractMva = static function (mixed $value): array {
				$str = trim((string)$value);
				if ($str === '') {
					return [];
				}
				if (str_contains($str, ',')) {
					return array_values(
						array_filter(
							array_map('trim', explode(',', $str)),
							static fn ($v) => $v !== ''
						)
					);
				}
				return [$str];
			};

			/**
			 * Sort result rows in PHP according to an ORDER BY string.
			 * Strips table prefixes from column names before looking up values.
			 */
			$applySort = static function (array $rows, string $orderBy) use ($splitByComma): array {
				if (empty($rows) || $orderBy === '') {
					return $rows;
				}
				$specs = [];
				foreach ($splitByComma($orderBy) as $part) {
					$part = trim($part);
					$dir  = 'ASC';
					if (preg_match('/\s+(ASC|DESC)$/i', $part, $dm)) {
						$dir  = strtoupper($dm[1]);
						$part = trim(substr($part, 0, -strlen($dm[0])));
					}
					// Strip optional table prefix (e.g. categories.name → name)
					if (str_contains($part, '.')) {
						[, $part] = explode('.', $part, 2);
					}
					$specs[] = ['field' => $part, 'dir' => $dir];
				}
				usort($rows, static function ($a, $b) use ($specs) {
					foreach ($specs as $s) {
						$va = $a[$s['field']] ?? null;
						$vb = $b[$s['field']] ?? null;
						if ($va === $vb) {
							continue;
						}
						$cmp = (is_numeric($va) && is_numeric($vb))
							? ($va <=> $vb)
							: strcmp((string)$va, (string)$vb);
						return $s['dir'] === 'DESC' ? -$cmp : $cmp;
					}
					return 0;
				});
				return $rows;
			};

			// ==================================================================
			// STEP 1 – PARSE THE MVA JOIN QUERY
			// ==================================================================

			// SELECT list (between SELECT and the first FROM … MVA JOIN)
			if (!preg_match('/^\s*SELECT\s+(.*?)\s+FROM\s+\w+\s+MVA\s+JOIN\b/is', $query, $m)) {
				throw new RuntimeException('MVA JOIN plugin: failed to parse SELECT list');
			}
			$selectList = trim($m[1]);

			// Main table (FROM) and join table (MVA JOIN)
			if (!preg_match('/\bFROM\s+(\w+)\s+MVA\s+JOIN\s+(\w+)\b/i', $query, $m)) {
				throw new RuntimeException('MVA JOIN plugin: failed to parse table names');
			}
			$mainTable = $m[1];
			$joinTable = $m[2];

			// ON condition – identify which side is the MVA field and which is the join key
			if (!preg_match('/\bON\s+([\w.]+)\s*=\s*([\w.]+)/i', $query, $m)) {
				throw new RuntimeException('MVA JOIN plugin: failed to parse ON condition');
			}
			[$onLeft, $onRight] = [$m[1], $m[2]];

			$leftTable  = '';
			$leftField  = $onLeft;
			if (str_contains($onLeft, '.')) {
				[$leftTable, $leftField] = explode('.', $onLeft, 2);
			}
			$rightTable = '';
			$rightField = $onRight;
			if (str_contains($onRight, '.')) {
				[$rightTable, $rightField] = explode('.', $onRight, 2);
			}

			// The MVA field lives on the main table; the join field lives on the join table
			if (strcasecmp($leftTable, $mainTable) === 0 || strcasecmp($rightTable, $joinTable) === 0) {
				$mvaField  = $leftField;
				$joinField = $rightField;
			} else {
				$mvaField  = $rightField;
				$joinField = $leftField;
			}

			// WHERE clause (stop before GROUP BY / ORDER BY / LIMIT / HAVING)
			$whereClause = '';
			if (preg_match('/\bWHERE\s+(.*?)(?=\s+(?:GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b|$)/is', $query, $m)) {
				// Collapse newlines/tabs to spaces so the AND splitter always sees ' AND '
				$whereClause = trim(preg_replace('/\s+/', ' ', $m[1]));
			}

			// GROUP BY
			$groupBy = '';
			if (preg_match('/\bGROUP\s+BY\s+(.*?)(?=\s+(?:ORDER\s+BY|LIMIT|HAVING)\b|$)/is', $query, $m)) {
				$groupBy = trim($m[1]);
			}

			// ORDER BY
			$orderBy = '';
			if (preg_match('/\bORDER\s+BY\s+(.*?)(?=\s+(?:LIMIT|HAVING)\b|$)/is', $query, $m)) {
				$orderBy = trim($m[1]);
			}

			// LIMIT [offset,] count
			$limitCount  = 0;
			$limitOffset = 0;
			if (preg_match('/\bLIMIT\s+(\d+)(?:\s*,\s*(\d+))?/i', $query, $m)) {
				if (isset($m[2]) && $m[2] !== '') {
					$limitOffset = (int)$m[1];
					$limitCount  = (int)$m[2];
				} else {
					$limitCount = (int)$m[1];
				}
			}

			file_put_contents(
				$logFile,
				"  Parsed: mainTable=$mainTable, joinTable=$joinTable, mvaField=$mvaField, joinField=$joinField\n" .
				"  WHERE: $whereClause\n" .
				"  GROUP BY: $groupBy | ORDER BY: $orderBy | LIMIT: offset=$limitOffset count=$limitCount\n",
				FILE_APPEND
			);

			// ==================================================================
			// STEP 2 – CLASSIFY WHERE CONDITIONS
			// Conditions prefixed with joinTable. go to the join query;
			// conditions prefixed with mainTable. go to the main query;
			// unqualified conditions default to the join table (most common case).
			// ==================================================================

			$joinTableConditions = [];
			$mainTableConditions = [];
			$joinTablePrefix     = strtolower($joinTable) . '.';
			$mainTablePrefix     = strtolower($mainTable) . '.';

			foreach ($splitWhereByAnd($whereClause) as $cond) {
				$condTrimmed = trim($cond);
				$hasJoin     = stripos($condTrimmed, $joinTable . '.') !== false;
				$hasMain     = stripos($condTrimmed, $mainTable . '.') !== false;
				if ($hasJoin && !$hasMain) {
					// Strip ALL occurrences of the join-table prefix
					$joinTableConditions[] = trim(str_ireplace($joinTable . '.', '', $condTrimmed));
				} elseif ($hasMain && !$hasJoin) {
					// Strip ALL occurrences of the main-table prefix
					$mainTableConditions[] = trim(str_ireplace($mainTable . '.', '', $condTrimmed));
				} elseif (!$hasJoin && !$hasMain) {
					// Unqualified → join table by convention
					$joinTableConditions[] = $condTrimmed;
				}
				// Mixed-table OR (both prefixes in one token) is skipped:
				// cannot be cleanly routed to either single-table query.
			}

			file_put_contents(
				$logFile,
				'  Join table WHERE: ' . implode(' AND ', $joinTableConditions) . "\n" .
				'  Main table WHERE: ' . implode(' AND ', $mainTableConditions) . "\n",
				FILE_APPEND
			);

			// ==================================================================
			// STEP 3 – PARSE SELECT LIST
			// Categorise each item as: COUNT(*), aggregate, join-table field,
			// or main-table field.
			// ==================================================================

			$hasCountStar          = false;
			$countStarAlias        = 'COUNT(*)';
			$joinTableSelectFields = [];  // [{column, alias}]
			$mainTableSelectFields = [];  // [{column, alias}]
			$mainTableAggExprs    = [];  // [{func, column, outputName, sqlExpr, sqlAlias}]
			$selectOrder          = [];  // preserves original SELECT column order

			foreach ($splitByComma($selectList) as $part) {
				$part = trim($part);

				// COUNT(*) [AS alias]
				if (preg_match('/^COUNT\s*\(\s*\*\s*\)(?:\s+AS\s+(\S+))?$/i', $part, $mm)) {
					$hasCountStar   = true;
					$countStarAlias = $mm[1] ?? 'COUNT(*)';
					$selectOrder[]  = ['type' => 'count'];
					continue;
				}

				// Aggregate functions on main-table fields: SUM(t.col), AVG(t.col), GROUP_CONCAT(t.col)
				if (preg_match('/^(SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(/i', $part)) {
					if (preg_match(
						'/^(SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(\s*(?:(\w+)\.)?\s*(\w+)\s*\)(?:\s+AS\s+(\w+))?$/i',
						$part, $mm
					)) {
						$tblPrefix = $mm[2] ?? '';
						$col       = $mm[3];
						$alias     = isset($mm[4]) && $mm[4] !== '' ? $mm[4] : null;
						if ($tblPrefix === '' || strcasecmp($tblPrefix, $mainTable) === 0) {
							$mainTableAggExprs[] = [
								'func'       => strtoupper($mm[1]),
								'column'     => $col,
								'outputName' => $alias ?? $part,
								'sqlExpr'    => strtoupper($mm[1]) . "({$col})",
								'sqlAlias'   => '_agg_' . count($mainTableAggExprs),
							];
							$selectOrder[] = ['type' => 'agg', 'idx' => count($mainTableAggExprs) - 1];
						}
					}
					continue;
				}

				// table.column [AS alias]
				if (preg_match('/^(\w+)\.(\w+)(?:\s+AS\s+(\w+))?$/i', $part, $mm)) {
					$tbl   = $mm[1];
					$col   = $mm[2];
					$alias = $mm[3] ?? null;
					if (strcasecmp($tbl, $joinTable) === 0) {
						$joinTableSelectFields[] = ['column' => $col, 'alias' => $alias];
						$selectOrder[]           = ['type' => 'join', 'idx' => count($joinTableSelectFields) - 1];
					} elseif (strcasecmp($tbl, $mainTable) === 0) {
						$mainTableSelectFields[] = ['column' => $col, 'alias' => $alias];
						$selectOrder[]           = ['type' => 'raw', 'idx' => count($mainTableSelectFields) - 1];
					}
					continue;
				}

				// Plain column name [AS alias] without table prefix – skip (ambiguous without schema)
			}

			// Mode: aggregation when COUNT(*) or GROUP BY is present
			$isAggregation = $hasCountStar || $groupBy !== '';

			// Per-category queries needed when SELECT references main-table fields/aggregates
			$needPerCategoryQuery = !empty($mainTableAggExprs) || !empty($mainTableSelectFields);

			file_put_contents(
				$logFile,
				'  Mode: ' . ($isAggregation ? 'AGGREGATION' : 'ROW') . "\n" .
				'  COUNT(*) alias: ' . $countStarAlias . "\n",
				FILE_APPEND
			);

			// ==================================================================
			// STEP 4 – DETERMINE JOIN TABLE COLUMNS TO FETCH
			// ==================================================================

			$joinFetchCols = [$joinField];

			foreach ($joinTableSelectFields as $f) {
				if (!in_array($f['column'], $joinFetchCols, true)) {
					$joinFetchCols[] = $f['column'];
				}
			}

			// Also fetch any GROUP BY fields that reference the join table
			if ($groupBy) {
				foreach ($splitByComma($groupBy) as $gbPart) {
					$gbPart = trim($gbPart);
					if (str_contains($gbPart, '.')) {
						[$gbTable, $gbCol] = explode('.', $gbPart, 2);
						if (strcasecmp($gbTable, $joinTable) === 0 && !in_array($gbCol, $joinFetchCols, true)) {
							$joinFetchCols[] = $gbCol;
						}
					}
				}
			}

			// ==================================================================
			// STEP 5 – EXECUTE JOIN TABLE QUERY
			// ==================================================================

			$joinSelectStr = implode(', ', $joinFetchCols);
			$joinWhereStr  = !empty($joinTableConditions)
				? 'WHERE ' . implode(' AND ', $joinTableConditions)
				: '';
			$joinQuery = "SELECT {$joinSelectStr} FROM {$joinTable} {$joinWhereStr} LIMIT 0, 10000";

			file_put_contents($logFile, "\n  [Join Table Query]: {$joinQuery}\n", FILE_APPEND);

			$joinResponse = $manticoreClient->sendRequest($joinQuery);
			if ($joinResponse->hasError()) {
				throw new RuntimeException('MVA JOIN: join table query failed: ' . $joinResponse->getError());
			}
			$joinRows = $joinResponse->getData();
			file_put_contents($logFile, '  Join table rows returned: ' . count($joinRows) . "\n", FILE_APPEND);

			if (empty($joinRows)) {
				file_put_contents($logFile, "  Empty join result – returning empty\n", FILE_APPEND);
				return TaskResult::withData([]);
			}

			// ==================================================================
			// STEP 6 – BUILD LOOKUP MAPS FROM JOIN TABLE RESULTS
			// ==================================================================

			$catRows      = [];      // index (0,1,2,...) => full row
			$catKeywords  = [];      // index => [kw1, kw2, ...]
			$allKwMap     = [];      // keyword => true  (deduplication set)
			$kwToCatIdxs  = [];      // keyword => [idx, ...]

			foreach (array_values($joinRows) as $idx => $row) {
				$catRows[$idx]    = $row;
				$keywords         = $extractMva($row[$joinField] ?? '');
				$catKeywords[$idx] = $keywords;
				foreach ($keywords as $kw) {
					$allKwMap[$kw]         = true;
					$kwToCatIdxs[$kw][]   = $idx;
				}
			}

			$allKeywords = array_keys($allKwMap);
			file_put_contents(
				$logFile,
				'  Join-table rows processed: ' . count($catRows) . "\n" .
				'  Unique keywords collected: ' . count($allKeywords) . "\n",
				FILE_APPEND
			);

			if (empty($allKeywords)) {
				file_put_contents($logFile, "  No keywords found – returning empty\n", FILE_APPEND);
				return TaskResult::withData([]);
			}

			$allKwList = implode(',', $allKeywords);

			// Build the main-table WHERE string
			$mainWhereParts   = $mainTableConditions;
			$mainWhereParts[] = "{$mvaField} IN ({$allKwList})";
			$mainWhereStr     = 'WHERE ' . implode(' AND ', $mainWhereParts);

			// ==================================================================
			// MODE A – AGGREGATION (COUNT(*) / GROUP BY)
			// Uses SUM(mvaField IN (kw1,...)) per join-table row.
			// Manticore returns a single aggregate row; we expand it back.
			// ==================================================================

			if ($isAggregation) {
				$sumExprs  = [];
				$validIdxs = [];

				foreach ($catRows as $idx => $catRow) {
					$kwList = implode(',', $catKeywords[$idx]);
					if ($kwList === '') {
						continue;
					}
					// Alias: _c0, _c1, _c2, ... (safe SQL identifiers)
					$sumExprs[]  = "SUM({$mvaField} IN ({$kwList})) AS `_c{$idx}`";
					$validIdxs[] = $idx;
				}

				if (empty($sumExprs)) {
					return TaskResult::withData([]);
				}

				$aggQuery = 'SELECT ' . implode(', ', $sumExprs)
					. " FROM {$mainTable} {$mainWhereStr}";

				file_put_contents($logFile, "\n  [Aggregation Query]: " . substr($aggQuery, 0, 1000) . "\n", FILE_APPEND);

				$aggResponse = $manticoreClient->sendRequest($aggQuery);
				if ($aggResponse->hasError()) {
					throw new RuntimeException('MVA JOIN: aggregation query failed: ' . $aggResponse->getError());
				}
				$aggData = $aggResponse->getData();
				$aggRow  = $aggData[0] ?? [];

				file_put_contents($logFile, '  Aggregation row: ' . json_encode($aggRow) . "\n", FILE_APPEND);

				// Identify matched categories (INNER JOIN semantics: count > 0)
				$matchedIdxMap = [];
				foreach ($validIdxs as $idx) {
					$count = (int)($aggRow["_c{$idx}"] ?? 0);
					if ($count > 0) {
						$matchedIdxMap[$idx] = $count;
					}
				}

				// Per-category queries: aggregates and raw fields run in separate queries
				// because Manticore rejects mixing aggregate and non-aggregate columns
				// in one query without GROUP BY.
				$catMainData = [];
				if ($needPerCategoryQuery && !empty($matchedIdxMap)) {
					foreach (array_keys($matchedIdxMap) as $idx) {
						$kwList = implode(',', $catKeywords[$idx]);
						if ($kwList === '') {
							continue;
						}
						$catWhereParts   = $mainTableConditions;
						$catWhereParts[] = "{$mvaField} IN ({$kwList})";
						$catWhereStr     = 'WHERE ' . implode(' AND ', $catWhereParts);
						$catRow          = [];

						// Aggregate functions (SUM, GROUP_CONCAT, AVG, etc.)
						if (!empty($mainTableAggExprs)) {
							$aggParts = [];
							foreach ($mainTableAggExprs as $agg) {
								$aggParts[] = $agg['sqlExpr'] . ' AS ' . $agg['sqlAlias'];
							}
							$aggQuery = 'SELECT ' . implode(', ', $aggParts) . " FROM {$mainTable} {$catWhereStr}";
							file_put_contents($logFile, "  [Per-cat agg idx={$idx}]: " . substr($aggQuery, 0, 500) . "\n", FILE_APPEND);
							$aggResp = $manticoreClient->sendRequest($aggQuery);
							if ($aggResp->hasError()) {
								file_put_contents($logFile, "  [Per-cat agg ERR idx={$idx}]: " . $aggResp->getError() . "\n", FILE_APPEND);
								throw new RuntimeException('MVA JOIN per-category query failed: ' . $aggResp->getError());
							} else {
								$aggRespData = $aggResp->getData();
								$catRow      = array_merge($catRow, $aggRespData[0] ?? []);
							}
						}

						// Raw (non-aggregate) main-table fields - pick any matching row
						if (!empty($mainTableSelectFields)) {
							$rawParts = [];
							foreach ($mainTableSelectFields as $f) {
								$rawParts[] = $f['column'] . ' AS _raw_' . $f['column'];
							}
							$rawQuery = 'SELECT ' . implode(', ', $rawParts) . " FROM {$mainTable} {$catWhereStr} LIMIT 1";
							file_put_contents($logFile, "  [Per-cat raw idx={$idx}]: " . substr($rawQuery, 0, 500) . "\n", FILE_APPEND);
							$rawResp = $manticoreClient->sendRequest($rawQuery);
							if ($rawResp->hasError()) {
								file_put_contents($logFile, "  [Per-cat raw ERR idx={$idx}]: " . $rawResp->getError() . "\n", FILE_APPEND);
								throw new RuntimeException('MVA JOIN per-category query failed: ' . $rawResp->getError());
							} else {
								$rawRespData = $rawResp->getData();
								$catRow      = array_merge($catRow, $rawRespData[0] ?? []);
							}
						}

						$catMainData[$idx] = $catRow;
						file_put_contents($logFile, "  [Per-cat result idx={$idx}]: " . json_encode($catRow) . "\n", FILE_APPEND);
					}
				}

				// Build result rows
				$resultRows = [];
				foreach ($matchedIdxMap as $idx => $count) {
					$catRow  = $catRows[$idx];
					$mainRow = $needPerCategoryQuery ? ($catMainData[$idx] ?? []) : [];
					$row     = [];
					foreach ($selectOrder as $spec) {
						switch ($spec['type']) {
							case 'count':
								$row[$countStarAlias] = $count;
								break;
							case 'join':
								$f       = $joinTableSelectFields[$spec['idx']];
								$colName = $f['alias'] ?? $f['column'];
								$row[$colName] = $catRow[$f['column']] ?? null;
								break;
							case 'agg':
								$agg = $mainTableAggExprs[$spec['idx']];
								$row[$agg['outputName']] = $mainRow[$agg['sqlAlias']] ?? null;
								break;
							case 'raw':
								$f       = $mainTableSelectFields[$spec['idx']];
								$colName = $f['alias'] ?? $f['column'];
								$row[$colName] = $mainRow['_raw_' . $f['column']] ?? null;
								break;
						}
					}
					$resultRows[] = $row;
				}

				file_put_contents($logFile, '  Result rows built: ' . count($resultRows) . "\n", FILE_APPEND);

				if ($orderBy) {
					$resultRows = $applySort($resultRows, $orderBy);
				}
				if ($limitCount > 0) {
					$resultRows = array_slice($resultRows, $limitOffset, $limitCount);
				}

				file_put_contents($logFile, '  Returning ' . count($resultRows) . " rows (aggregation mode)\n\n", FILE_APPEND);
				$result = TaskResult::withData($resultRows);
				foreach (array_keys($resultRows[0] ?? []) as $colName) {
					$val = $resultRows[0][$colName] ?? null;
					$result->column($colName, is_int($val) ? Column::Long : Column::String);
				}
				return $result;
			}

			// ==================================================================
			// MODE B – ROW QUERY (individual rows; main-table fields in SELECT)
			// Fetches main table rows then expands MVA values to one result row
			// per (article, matching-category) pair.
			// ==================================================================

			$mainFetchCols = [$mvaField];  // always need the MVA field for matching
			foreach ($mainTableSelectFields as $f) {
				if (!in_array($f['column'], $mainFetchCols, true)) {
					$mainFetchCols[] = $f['column'];
				}
			}

			$mainSelectStr = implode(', ', $mainFetchCols);
			// Fetch enough rows to cover the requested LIMIT after expansion.
			// Default cap of 50,000 prevents runaway memory usage.
			$fetchLimit = $limitCount > 0 ? min($limitCount * 100, 50000) : 50000;
			$mainQuery  = "SELECT {$mainSelectStr} FROM {$mainTable} {$mainWhereStr} LIMIT 0, {$fetchLimit}";

			file_put_contents($logFile, "\n  [Main Table Query]: " . substr($mainQuery, 0, 500) . "\n", FILE_APPEND);

			$mainResponse = $manticoreClient->sendRequest($mainQuery);
			if ($mainResponse->hasError()) {
				throw new RuntimeException('MVA JOIN: main table query failed: ' . $mainResponse->getError());
			}
			$articleRows = $mainResponse->getData();
			file_put_contents($logFile, '  Main table rows: ' . count($articleRows) . "\n", FILE_APPEND);

			// Expand: one output row per (article, matching join-table row)
			$resultRows = [];
			foreach ($articleRows as $artRow) {
				$artKeywords = $extractMva($artRow[$mvaField] ?? '');

				// Collect all matching join-table indices, deduplicated per join row
				$matchedIdxs = [];
				foreach ($artKeywords as $kw) {
					if (isset($kwToCatIdxs[$kw])) {
						foreach ($kwToCatIdxs[$kw] as $idx) {
							$matchedIdxs[$idx] = true;
						}
					}
				}

				foreach (array_keys($matchedIdxs) as $idx) {
					$catRow = $catRows[$idx];
					$row    = [];

					foreach ($mainTableSelectFields as $f) {
						$colName      = $f['alias'] ?? $f['column'];
						$row[$colName] = $artRow[$f['column']] ?? null;
					}
					foreach ($joinTableSelectFields as $f) {
						$colName      = $f['alias'] ?? $f['column'];
						$row[$colName] = $catRow[$f['column']] ?? null;
					}
					$resultRows[] = $row;
				}
			}

			file_put_contents($logFile, '  Expanded rows: ' . count($resultRows) . "\n", FILE_APPEND);

			if ($orderBy) {
				$resultRows = $applySort($resultRows, $orderBy);
			}
			if ($limitCount > 0) {
				$resultRows = array_slice($resultRows, $limitOffset, $limitCount);
			}

			file_put_contents($logFile, '  Returning ' . count($resultRows) . " rows (row mode)\n\n", FILE_APPEND);
			$result = TaskResult::withData($resultRows);
			foreach (array_keys($resultRows[0] ?? []) as $colName) {
				$val = $resultRows[0][$colName] ?? null;
				$result->column($colName, is_int($val) ? Column::Long : Column::String);
			}
			return $result;
		};

		return Task::create($taskFn, [$this->payload, $this->manticoreClient])->run();
	}
}
