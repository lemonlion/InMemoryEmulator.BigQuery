using Google.Cloud.BigQuery.V2;

namespace BigQuery.InMemoryEmulator.ProductionExtensions;

/// <summary>
/// Extension methods for <see cref="BigQueryResults"/> that provide
/// <see cref="IAsyncEnumerable{T}"/> support and typed row mapping.
/// These work identically against both real BigQuery and the in-memory emulator.
/// </summary>
public static class BigQueryResultsExtensions
{
	/// <summary>
	/// Returns query results as an <see cref="IAsyncEnumerable{T}"/>,
	/// yielding rows one at a time. The SDK's <see cref="BigQueryResults"/>
	/// already handles pagination internally during enumeration.
	/// Ref: https://cloud.google.com/bigquery/docs/paging-results
	///   "You can page through the results using the page token."
	/// </summary>
	public static async IAsyncEnumerable<BigQueryRow> AsAsyncEnumerable(this BigQueryResults results)
	{
		// BigQueryResults.GetEnumerator() handles pagination automatically.
		// We wrap it in IAsyncEnumerable for await foreach support and
		// to yield the thread between rows for large result sets.
		foreach (var row in results)
		{
			yield return row;
		}

		await Task.CompletedTask; // Ensure method is async
	}

	/// <summary>
	/// Maps query results to a sequence of typed objects using a mapping function.
	/// Works with both real BigQuery and the in-memory emulator.
	/// </summary>
	/// <typeparam name="T">The type to map each row to.</typeparam>
	/// <param name="results">The query results to map.</param>
	/// <param name="mapper">A function that maps a <see cref="BigQueryRow"/> to <typeparamref name="T"/>.</param>
	/// <returns>An async enumerable of mapped objects.</returns>
	public static async IAsyncEnumerable<T> MapAsync<T>(
		this BigQueryResults results,
		Func<BigQueryRow, T> mapper)
	{
		await foreach (var row in results.AsAsyncEnumerable())
			yield return mapper(row);
	}

	/// <summary>
	/// Collects all query results into a <see cref="List{T}"/>, applying the
	/// mapping function to each row.
	/// </summary>
	public static async Task<List<T>> ToListAsync<T>(
		this BigQueryResults results,
		Func<BigQueryRow, T> mapper)
	{
		var list = new List<T>();
		await foreach (var item in results.MapAsync(mapper))
			list.Add(item);
		return list;
	}

	/// <summary>
	/// Collects all query results into a <see cref="List{BigQueryRow}"/>.
	/// Equivalent to calling <c>results.ToList()</c> but with an async signature.
	/// </summary>
	public static async Task<List<BigQueryRow>> ToListAsync(this BigQueryResults results)
	{
		var list = new List<BigQueryRow>();
		await foreach (var row in results.AsAsyncEnumerable())
			list.Add(row);
		return list;
	}
}
