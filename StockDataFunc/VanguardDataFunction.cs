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

public class VanguardDataFunction
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string ConnStr = Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING") ?? "";

    // =====================================================================
    // TIMER TRIGGER: Daily Vanguard prices - runs Mon-Fri at 7:30 PM UTC
    // =====================================================================
    [Function("DailyVanguardPrices")]
    public async Task RunDaily([TimerTrigger("0 30 19 * * 1-5")] TimerInfo timer, FunctionContext context)
    {
        var log = context.GetLogger("DailyVanguardPrices");
        log.LogInformation($"Daily Vanguard price timer triggered at {DateTime.UtcNow}");

        var symbols = await GetVanguardSymbolsFromDatabase(log);
        log.LogInformation($"Processing {symbols.Count} Vanguard symbols");

        foreach (var symbol in symbols)
        {
            await FetchAndStoreVanguard(symbol, "5d", log);
            await Task.Delay(2000);
        }

        log.LogInformation("Daily Vanguard update complete");
    }

    // =====================================================================
    // HTTP TRIGGER: Manual runs and historical loads
    // Usage:
    //   /api/vanguard              - fetch 5 days for all symbols from SQL
    //   /api/vanguard/VTSAX        - fetch single fund
    //   /api/vanguard?range=5y     - fetch 5 years for all
    //   /api/vanguard?range=max    - fetch max history
    // Valid ranges: 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
    // =====================================================================
    [Function("GetVanguardData")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "vanguard/{symbol?}")] 
        HttpRequestData req,
        string? symbol,
        FunctionContext context)
    {
        var log = context.GetLogger("VanguardDataFunction");

        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var range = query["range"] ?? "5d";

        log.LogInformation($"Fetching Vanguard data. Symbol: {symbol ?? "all"}, Range: {range}");

        var results = new List<object>();

        if (!string.IsNullOrEmpty(symbol))
        {
            var result = await FetchAndStoreVanguard(symbol.ToUpper(), range, log);
            results.Add(result);
        }
        else
        {
            var symbols = await GetVanguardSymbolsFromDatabase(log);
            log.LogInformation($"Processing {symbols.Count} Vanguard symbols");

            foreach (var sym in symbols)
            {
                var result = await FetchAndStoreVanguard(sym, range, log);
                results.Add(result);
                await Task.Delay(2000);
            }
        }

        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            timestamp = DateTime.UtcNow,
            range = range,
            symbol_count = results.Count,
            message = "Vanguard data fetched and written to database",
            results
        });

        return response;
    }

    // =====================================================================
    // HTTP TRIGGER: Vanguard SQL health check
    // =====================================================================
    [Function("TestVanguardSQL")]
    public async Task<HttpResponseData> TestVanguardSQL(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "testvanguardsql")] 
        HttpRequestData req,
        FunctionContext context)
    {
        var log = context.GetLogger("VanguardDataFunction");
        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        try
        {
            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                SELECT 
                    (SELECT COUNT(*) FROM rob.vanguard_prices) as total_rows,
                    (SELECT COUNT(DISTINCT symbol) FROM rob.vanguard_prices) as symbol_count,
                    (SELECT MAX(price_date) FROM rob.vanguard_prices) as latest_date,
                    (SELECT MIN(price_date) FROM rob.vanguard_prices) as earliest_date", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                await response.WriteAsJsonAsync(new
                {
                    status = "connected",
                    total_rows = reader[0],
                    symbol_count = reader[1],
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
    // HELPER: Get Vanguard symbols from SQL view
    // =====================================================================
    private async Task<List<string>> GetVanguardSymbolsFromDatabase(ILogger log)
    {
        var symbols = new List<string>();
        try
        {
            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand("SELECT symbol FROM rob.vw_distinct_vanguard_symbols", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var sym = reader.GetString(0).Trim().ToUpper();
                if (!string.IsNullOrEmpty(sym))
                    symbols.Add(sym);
            }
            log.LogInformation($"Loaded {symbols.Count} Vanguard symbols from database");
        }
        catch (Exception ex)
        {
            log.LogError($"Error loading Vanguard symbols from DB: {ex.Message}");
        }
        return symbols;
    }

    // =====================================================================
    // CORE: Fetch from Yahoo Finance and store in SQL
    // =====================================================================
    private async Task<object> FetchAndStoreVanguard(string symbol, string range, ILogger log)
    {
        try
        {
            var encodedSymbol = Uri.EscapeDataString(symbol);
            var url = $"https://query1.finance.yahoo.com/v8/finance/chart/{encodedSymbol}?interval=1d&range={range}";

            log.LogInformation($"Fetching {symbol} range={range}");

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

            // Get fund name from meta
            var fundName = result["meta"]?["longName"]?.ToString() 
                        ?? result["meta"]?["shortName"]?.ToString() 
                        ?? symbol;

            var timestamps = result["timestamp"]?.ToObject<long[]>();
            var quotes = result["indicators"]?["quote"]?[0];

            if (timestamps == null || quotes == null)
            {
                log.LogWarning($"Missing data for {symbol}");
                return new { symbol, error = "Missing data" };
            }

            var closes = quotes["close"]?.ToObject<decimal?[]>() ?? Array.Empty<decimal?>();

            int rowsWritten = 0;

            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();

            for (int i = 0; i < timestamps.Length; i++)
            {
                if (i >= closes.Length || closes[i] == null) continue;

                var date = DateTimeOffset.FromUnixTimeSeconds(timestamps[i]).UtcDateTime.Date;

                var sql = @"
                    MERGE rob.vanguard_prices AS target
                    USING (SELECT @symbol, @date) AS source (symbol, price_date)
                    ON target.symbol = source.symbol AND target.price_date = source.price_date
                    WHEN MATCHED THEN UPDATE SET 
                        fund_name = @fund_name,
                        nav_price = @nav_price
                    WHEN NOT MATCHED THEN INSERT 
                        (symbol, fund_name, price_date, nav_price)
                    VALUES (@symbol, @fund_name, @date, @nav_price);";

                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@symbol", symbol);
                cmd.Parameters.AddWithValue("@fund_name", fundName);
                cmd.Parameters.AddWithValue("@date", date);
                cmd.Parameters.AddWithValue("@nav_price", closes[i].HasValue ? (object)closes[i]!.Value : DBNull.Value);
                await cmd.ExecuteNonQueryAsync();
                rowsWritten++;
            }

            log.LogInformation($"Wrote {rowsWritten} rows for {symbol} ({fundName})");
            return new { symbol, fund_name = fundName, rows_written = rowsWritten };
        }
        catch (Exception ex)
        {
            log.LogError($"Error fetching {symbol}: {ex.Message}");
            return new { symbol, error = ex.Message };
        }
    }
}
