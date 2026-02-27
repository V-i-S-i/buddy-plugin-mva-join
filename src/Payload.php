<?php declare(strict_types=1);

/*
 * Manticore Buddy Plugin: MVA JOIN
 * Simulates JOIN on Multi-Value Attribute fields by decomposing the query
 * into a join-table fetch + PHP-side aggregation/expansion.
 */

namespace Manticoresearch\Buddy\Plugin\MvaJoin;

use Manticoresearch\Buddy\Core\Network\Request;
use Manticoresearch\Buddy\Core\Plugin\BasePayload;

/**
 * Detects queries containing the MVA JOIN clause and stores the raw query.
 *
 * Supported query shape:
 *   SELECT ... FROM mainTable
 *   MVA JOIN joinTable ON mainTable.mvaField = joinTable.joinField
 *   [WHERE ...]  [GROUP BY ...]  [ORDER BY ...]  [LIMIT ...]
 */
final class Payload extends BasePayload
{
	public string $query;

	/**
	 * Plugin description shown in SHOW PLUGINS.
	 */
	public static function getInfo(): string
	{
		return 'Simulates MVA JOIN by executing the join table query separately and aggregating/expanding results in PHP';
	}

	/**
	 * Fast detection: SELECT query containing "MVA JOIN".
	 */
	public static function hasMatch(Request $request): bool
	{
		$logFile = '/tmp/mva-join-debug.log';

		try {
			$query = self::getQuery($request);

			$debugInfo = sprintf(
				"[%s] hasMatch() called!\n  payload: %s\n",
				date('Y-m-d H:i:s'),
				substr($query, 0, 200)
			);
			file_put_contents($logFile, $debugInfo, FILE_APPEND);

			if (!preg_match('/^\s*SELECT\s+/i', $query)) {
				file_put_contents($logFile, "  Not a SELECT query\n\n", FILE_APPEND);
				return false;
			}

			$hasMatch = preg_match('/\bMVA\s+JOIN\b/i', $query) > 0;
			file_put_contents($logFile, '  Has MVA JOIN: ' . ($hasMatch ? 'YES' : 'NO') . "\n\n", FILE_APPEND);

			return $hasMatch;
		} catch (\Throwable $e) {
			file_put_contents($logFile, '  ERROR in hasMatch: ' . $e->getMessage() . "\n\n", FILE_APPEND);
			return false;
		}
	}

	/**
	 * Create payload from request – just stores the raw query string.
	 */
	public static function fromRequest(Request $request): static
	{
		$payload = new static();
		$payload->query = self::getQuery($request);
		return $payload;
	}

	/**
	 * Extract the SQL query string from various request payload formats.
	 */
	protected static function getQuery(Request $request): string
	{
		$payload = $request->payload;

		if (is_string($payload)) {
			return trim($payload);
		}

		if (is_array($payload) && isset($payload['query'])) {
			return trim($payload['query']);
		}

		return '';
	}
}
