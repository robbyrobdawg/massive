using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

public class IndexDataFunction
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string ConnStr = Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING") ?? "";

    // Yahoo Finance symbols for major indices
    private static readonly Dictionary<string, string> Indices = new Dictionary<string, string>
    {
        { "^GSPC",  "S&P 500" },
        { "^DJI",   "Dow Jones Industrial Average" },
        { "^NDX",   "NASDAQ 100" },
        { "^IXIC",  "NASDAQ Composite" },
        { "^RUT",   "Russell 2000" },
        { "^VIX",   "CBOE Volatility Index" }
    };

    // =====================================================================
    // TIMER TRIGGER: Daily index prices - runs Mon-Fri at 7:00 PM UTC
    // =====================================================================
    [Function("DailyIndexPrices")]
    public async Task RunDaily([TimerTrigger("0 0 19 * * 1-5")] TimerInfo timer, FunctionContext context)
    {
        var log = context.GetLogger("DailyIndexPrices");
        log.LogInformation($"Daily index price timer triggered at {DateTime.UtcNow}");

        foreach (var index in Indices)
        {
            await FetchAndStoreIndex(index.Key, index.Value, "5d", log);
            await Task.Delay(2000); // 2 second delay between calls
        }

        log.LogInformation("Daily index update complete");
    }

    // =====================================================================
    // HTTP TRIGGER: Manual runs and historical loads
    // Usage:
    //   /api/indices              - fetch 5 days for all indices
    //   /api/indices/%5EGSPC      - fetch single index (%5E = ^)
    //   /api/indices?range=5y     - fetch 5 years for all indices
    //   /api/indices?range=max    - fetch max history for all indices
    // Valid ranges: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
    // =====================================================================
    [Function("GetIndexData")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "indices/{symbol?}")] 
        HttpRequestData req,
        string? symbol,
        FunctionContext context)
    {
        var log = context.GetLogger("IndexDataFunction");

        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var range = query["range"] ?? "5d";

        log.LogInformation($"Fetching index data. Symbol: {symbol ?? "all"}, Range: {range}");

        var results = new List<object>();

        if (!string.IsNullOrEmpty(symbol))
        {
            var name = Indices.ContainsKey(symbol) ? Indices[symbol] : symbol;
            var result = await FetchAndStoreIndex(symbol.ToUpper(), name, range, log);
            results.Add(result);
        }
        else
        {
            foreach (var index in Indices)
            {
                var result = await FetchAndStoreIndex(index.Key, index.Value, range, log);
                results.Add(result);
                await Task.Delay(2000); // 2 second delay to avoid rate limiting
            }
        }

        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            timestamp = DateTime.UtcNow,
            range = range,
            index_count = results.Count,
            message = "Index data fetched and written to database",
            results
        });

        return response;
    }

    // =====================================================================
    // HTTP TRIGGER: Index SQL health check
    // =====================================================================
    [Function("TestIndexSQL")]
    public async Task<HttpResponseData> TestIndexSQL(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "testindexsql")] 
        HttpRequestData req,
        FunctionContext context)
    {
        var log = context.GetLogger("IndexDataFunction");
        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        try
        {
            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                SELECT 
                    (SELECT COUNT(*) FROM rob.index_prices) as total_rows,
                    (SELECT COUNT(DISTINCT symbol) FROM rob.index_prices) as index_count,
                    (SELECT MAX(price_date) FROM rob.index_prices) as latest_date,
                    (SELECT MIN(price_date) FROM rob.index_prices) as earliest_date", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                await response.WriteAsJsonAsync(new
                {
                    status = "connected",
                    total_rows = reader[0],
                    index_count = reader[1],
                    latest_date = reader[2],
                    earliest_date = reader[3]
                });
            }
        }
        catch (Exception ex)
        {
            await response.WriteAsJsonAsync(new { status = "error", message = ex.Message });
        }
        return response;
    }

    // =====================================================================
    // CORE: Fetch from Yahoo Finance and store in SQL
    // =====================================================================
    private async Task<object> FetchAndStoreIndex(string symbol, string indexName, string range, ILogger log)
    {
        try
        {
            var encodedSymbol = Uri.EscapeDataString(symbol);
            var url = $"https://query1.finance.yahoo.com/v8/finance/chart/{encodedSymbol}?interval=1d&range={range}";

            log.LogInformation($"Fetching {symbol} ({indexName}) range={range}");

            // Use browser-like headers to avoid Yahoo Finance rate limiting
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
            request.Headers.Add("Accept", "application/json");
            request.Headers.Add("Accept-Language", "en-US,en;q=0.9");
            var httpResponse = await client.SendAsync(request);
            var responseStr = await httpResponse.Content.ReadAsStringAsync();

            var json = JObject.Parse(responseStr);

            var result = json["chart"]?["result"]?[0];
            if (result == null)
            {
                log.LogWarning($"No data returned for {symbol}");
                return new { symbol, error = "No data returned" };
            }

            var timestamps = result["timestamp"]?.ToObject<long[]>();
            var quotes = result["indicators"]?["quote"]?[0];

            if (timestamps == null || quotes == null)
            {
                log.LogWarning($"Missing timestamp or quote data for {symbol}");
                return new { symbol, error = "Missing data" };
            }

            var opens   = quotes["open"]?.ToObject<decimal?[]>()  ?? Array.Empty<decimal?>();
            var highs   = quotes["high"]?.ToObject<decimal?[]>()  ?? Array.Empty<decimal?>();
            var lows    = quotes["low"]?.ToObject<decimal?[]>()   ?? Array.Empty<decimal?>();
            var closes  = quotes["close"]?.ToObject<decimal?[]>() ?? Array.Empty<decimal?>();
            var volumes = quotes["volume"]?.ToObject<long?[]>()   ?? Array.Empty<long?>();

            int rowsWritten = 0;

            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();

            for (int i = 0; i < timestamps.Length; i++)
            {
                if (i >= closes.Length || closes[i] == null) continue;

                var date = DateTimeOffset.FromUnixTimeSeconds(timestamps[i]).UtcDateTime.Date;

                var sql = @"
                    MERGE rob.index_prices AS target
                    USING (SELECT @symbol, @date) AS source (symbol, price_date)
                    ON target.symbol = source.symbol AND target.price_date = source.price_date
                    WHEN MATCHED THEN UPDATE SET 
                        index_name = @index_name,
                        open_price = @open, high_price = @high, 
                        low_price = @low, close_price = @close, volume = @volume
                    WHEN NOT MATCHED THEN INSERT 
                        (symbol, index_name, price_date, open_price, high_price, low_price, close_price, volume)
                    VALUES (@symbol, @index_name, @date, @open, @high, @low, @close, @volume);";

                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@symbol", symbol);
                cmd.Parameters.AddWithValue("@index_name", indexName);
                cmd.Parameters.AddWithValue("@date", date);
                cmd.Parameters.AddWithValue("@open",   i < opens.Length   && opens[i].HasValue   ? (object)opens[i]!.Value   : DBNull.Value);
                cmd.Parameters.AddWithValue("@high",   i < highs.Length   && highs[i].HasValue   ? (object)highs[i]!.Value   : DBNull.Value);
                cmd.Parameters.AddWithValue("@low",    i < lows.Length    && lows[i].HasValue    ? (object)lows[i]!.Value    : DBNull.Value);
                cmd.Parameters.AddWithValue("@close",  closes[i].HasValue ? (object)closes[i]!.Value : DBNull.Value);
                cmd.Parameters.AddWithValue("@volume", i < volumes.Length && volumes[i].HasValue ? (object)volumes[i]!.Value : DBNull.Value);
                await cmd.ExecuteNonQueryAsync();
                rowsWritten++;
            }

            log.LogInformation($"Wrote {rowsWritten} rows for {symbol}");
            return new { symbol, index_name = indexName, rows_written = rowsWritten };
        }
        catch (Exception ex)
        {
            log.LogError($"Error fetching {symbol}: {ex.Message}");
            return new { symbol, error = ex.Message };
        }
    }
}