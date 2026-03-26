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

public class StockDataFunction
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string ApiKey = Environment.GetEnvironmentVariable("MASSIVE_API_KEY") ?? "";
    private static readonly string BaseUrl = "https://api.polygon.io";
    private static readonly string ConnStr = Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING") ?? "";

    private static readonly bool RateLimitMode = bool.Parse(
        Environment.GetEnvironmentVariable("RATE_LIMIT_MODE") ?? "false");

    private static readonly List<string> DefaultTickers = new List<string>
    {
        "AAPL", "MSFT", "VOO", "VTI"
    };

    // =====================================================================
    // TIMER TRIGGER: Daily prices batch 1 (even-indexed tickers)
    // Runs Mon-Fri at 6:30 PM UTC
    // =====================================================================
    [Function("DailyStockPrices1")]
    public async Task RunDaily1([TimerTrigger("0 30 18 * * 1-5")] TimerInfo timer, FunctionContext context)
    {
        var log = context.GetLogger("DailyStockPrices1");
        log.LogInformation($"Daily stock price batch 1 triggered at {DateTime.UtcNow}");

        var fromDate = DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd");
        var toDate = fromDate;

        var allTickers = await GetTickersFromDatabase(log);
        var tickers = allTickers.Where((t, i) => i % 2 == 0).ToList();
        log.LogInformation($"Batch 1: Processing {tickers.Count} tickers for {fromDate}");

        var results = new List<object>();
        foreach (var t in tickers)
        {
            var result = await GetTickerData(t, fromDate, toDate, skipNews: true, log);
            results.Add(result);
            if (RateLimitMode) await Task.Delay(12000);
        }

        await WriteResultsToDatabase(results.ToArray(), skipNews: true, log);
        log.LogInformation("Daily price batch 1 complete");
    }

    // =====================================================================
    // TIMER TRIGGER: Daily prices batch 2 (odd-indexed tickers)
    // Runs Mon-Fri at 6:45 PM UTC (15 min after batch 1)
    // =====================================================================
    [Function("DailyStockPrices2")]
    public async Task RunDaily2([TimerTrigger("0 45 18 * * 1-5")] TimerInfo timer, FunctionContext context)
    {
        var log = context.GetLogger("DailyStockPrices2");
        log.LogInformation($"Daily stock price batch 2 triggered at {DateTime.UtcNow}");

        var fromDate = DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd");
        var toDate = fromDate;

        var allTickers = await GetTickersFromDatabase(log);
        var tickers = allTickers.Where((t, i) => i % 2 != 0).ToList();
        log.LogInformation($"Batch 2: Processing {tickers.Count} tickers for {fromDate}");

        var results = new List<object>();
        foreach (var t in tickers)
        {
            var result = await GetTickerData(t, fromDate, toDate, skipNews: true, log);
            results.Add(result);
            if (RateLimitMode) await Task.Delay(12000);
        }

        await WriteResultsToDatabase(results.ToArray(), skipNews: true, log);
        log.LogInformation("Daily price batch 2 complete");
    }

    // =====================================================================
    // TIMER TRIGGER: Weekly news - runs every Sunday at 8:00 AM UTC
    // =====================================================================
    [Function("WeeklyStockNews")]
    public async Task RunWeeklyNews([TimerTrigger("0 0 8 * * 0")] TimerInfo timer, FunctionContext context)
    {
        var log = context.GetLogger("WeeklyStockNews");
        log.LogInformation($"Weekly news timer triggered at {DateTime.UtcNow}");

        var fromDate = DateTime.UtcNow.AddDays(-7).ToString("yyyy-MM-dd");
        var toDate = DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd");

        var tickers = await GetTickersFromDatabase(log);
        log.LogInformation($"Processing news for {tickers.Count} tickers");

        var results = new List<object>();
        foreach (var t in tickers)
        {
            try
            {
                if (RateLimitMode) await Task.Delay(12000);
                var news = await GetStockNews(t);
                results.Add(new { ticker = t, history = (object?)null, news });
            }
            catch (Exception ex)
            {
                log.LogError($"Error fetching news for {t}: {ex.Message}");
            }
        }

        await WriteResultsToDatabase(results.ToArray(), skipNews: false, log);
        log.LogInformation("Weekly news update complete");
    }

    // =====================================================================
    // HTTP TRIGGER: Manual runs and testing
    // =====================================================================
    [Function("GetStockData")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "stock/{ticker?}")] 
        HttpRequestData req,
        string? ticker,
        FunctionContext context)
    {
        var log = context.GetLogger("StockDataFunction");
        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var fromDate = query["from"] ?? DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd");
        var toDate = query["to"] ?? DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd");
        var skipNews = bool.Parse(query["skip_news"] ?? "false");

        List<string> tickers;
        var tickerList = query["tickers"];

        if (!string.IsNullOrEmpty(ticker))
            tickers = new List<string> { ticker.ToUpper() };
        else if (!string.IsNullOrEmpty(tickerList))
            tickers = tickerList.Split(',').Select(t => t.Trim().ToUpper()).ToList();
        else
            tickers = await GetTickersFromDatabase(log);

        log.LogInformation($"Processing {tickers.Count} tickers from {fromDate} to {toDate}. Skip news: {skipNews}");

        var results = new List<object>();
        foreach (var t in tickers)
        {
            var result = await GetTickerData(t, fromDate, toDate, skipNews, log);
            results.Add(result);
            if (RateLimitMode) await Task.Delay(12000);
        }

        await WriteResultsToDatabase(results.ToArray(), skipNews, log);

        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            timestamp = DateTime.UtcNow,
            from = fromDate,
            to = toDate,
            ticker_count = tickers.Count,
            skip_news = skipNews,
            rate_limit_mode = RateLimitMode,
            message = "Data fetched and written to database"
        });

        return response;
    }

    // =====================================================================
    // HTTP TRIGGER: SQL health check
    // =====================================================================
    [Function("TestSQL")]
    public async Task<HttpResponseData> TestSQL(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "testsql")] 
        HttpRequestData req,
        FunctionContext context)
    {
        var log = context.GetLogger("StockDataFunction");
        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        try
        {
            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                SELECT 
                    (SELECT COUNT(*) FROM rob.stock_prices) as price_rows,
                    (SELECT COUNT(*) FROM rob.stock_news) as news_rows,
                    (SELECT COUNT(DISTINCT symbol) FROM rob.stock_prices) as symbols_with_prices,
                    (SELECT MAX(price_date) FROM rob.stock_prices) as latest_price_date", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                await response.WriteAsJsonAsync(new
                {
                    status = "connected",
                    price_rows = reader[0],
                    news_rows = reader[1],
                    symbols_with_prices = reader[2],
                    latest_price_date = reader[3]
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
    // HELPER METHODS
    // =====================================================================
    private async Task<List<string>> GetTickersFromDatabase(ILogger log)
    {
        var tickers = new List<string>();
        try
        {
            if (string.IsNullOrEmpty(ConnStr))
            {
                log.LogWarning("SQL_CONNECTION_STRING not set, using default tickers");
                return DefaultTickers;
            }
            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand("SELECT symbol FROM rob.vw_distinct_symbols", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var symbol = reader.GetString(0).Trim().ToUpper();
                if (!string.IsNullOrEmpty(symbol))
                    tickers.Add(symbol);
            }
            log.LogInformation($"Loaded {tickers.Count} tickers from database");
        }
        catch (Exception ex)
        {
            log.LogError($"Error loading tickers from DB: {ex.Message}");
            return DefaultTickers;
        }
        return tickers.Count > 0 ? tickers : DefaultTickers;
    }

    private async Task WriteResultsToDatabase(object[] results, bool skipNews, ILogger log)
    {
        try
        {
            using var conn = new SqlConnection(ConnStr);
            await conn.OpenAsync();

            foreach (var result in results)
            {
                var type = result.GetType();
                var tickerProp = type.GetProperty("ticker");
                var historyProp = type.GetProperty("history");
                var newsProp = type.GetProperty("news");
                var errorProp = type.GetProperty("error");

                if (errorProp?.GetValue(result) != null) continue;

                var symbol = tickerProp?.GetValue(result)?.ToString();
                if (string.IsNullOrEmpty(symbol)) continue;

                var history = historyProp?.GetValue(result) as System.Collections.IEnumerable;
                if (history != null)
                {
                    foreach (var bar in history)
                    {
                        var barType = bar.GetType();
                        var date = barType.GetProperty("date")?.GetValue(bar)?.ToString();
                        var open = barType.GetProperty("open")?.GetValue(bar);
                        var high = barType.GetProperty("high")?.GetValue(bar);
                        var low = barType.GetProperty("low")?.GetValue(bar);
                        var close = barType.GetProperty("close")?.GetValue(bar);
                        var volume = barType.GetProperty("volume")?.GetValue(bar);

                        var sql = @"
                            MERGE rob.stock_prices AS target
                            USING (SELECT @symbol, @date) AS source (symbol, price_date)
                            ON target.symbol = source.symbol AND target.price_date = source.price_date
                            WHEN MATCHED THEN UPDATE SET 
                                open_price = @open, high_price = @high, 
                                low_price = @low, close_price = @close, volume = @volume
                            WHEN NOT MATCHED THEN INSERT 
                                (symbol, price_date, open_price, high_price, low_price, close_price, volume)
                            VALUES (@symbol, @date, @open, @high, @low, @close, @volume);";

                        using var cmd = new SqlCommand(sql, conn);
                        cmd.Parameters.AddWithValue("@symbol", symbol);
                        cmd.Parameters.AddWithValue("@date", date ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@open", open ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@high", high ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@low", low ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@close", close ?? (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@volume", volume ?? (object)DBNull.Value);
                        await cmd.ExecuteNonQueryAsync();
                    }
                }

                if (!skipNews)
                {
                    var news = newsProp?.GetValue(result) as System.Collections.IEnumerable;
                    if (news != null)
                    {
                        foreach (var article in news)
                        {
                            var articleType = article.GetType();
                            var title = articleType.GetProperty("title")?.GetValue(article)?.ToString();
                            var published = articleType.GetProperty("published")?.GetValue(article)?.ToString();
                            var source = articleType.GetProperty("source")?.GetValue(article)?.ToString();
                            var articleUrl = articleType.GetProperty("url")?.GetValue(article)?.ToString();
                            var sentiment = articleType.GetProperty("sentiment")?.GetValue(article)?.ToString();

                            var sql = @"
                                MERGE rob.stock_news AS target
                                USING (SELECT @symbol, @url) AS source (symbol, article_url)
                                ON target.symbol = source.symbol AND target.article_url = source.article_url
                                WHEN NOT MATCHED THEN INSERT 
                                    (symbol, title, published_date, source, article_url, sentiment)
                                VALUES (@symbol, @title, @published, @source, @url, @sentiment);";

                            using var cmd = new SqlCommand(sql, conn);
                            cmd.Parameters.AddWithValue("@symbol", symbol);
                            cmd.Parameters.AddWithValue("@title", (object?)title ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@published", (object?)published ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@source", (object?)source ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@url", (object?)articleUrl ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@sentiment", (object?)sentiment ?? DBNull.Value);
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }
                }

                log.LogInformation($"Wrote data for {symbol} to database");
            }
        }
        catch (Exception ex)
        {
            log.LogError($"Error writing to database: {ex.Message}");
        }
    }

    private async Task<object> GetTickerData(string ticker, string fromDate, string toDate, bool skipNews, ILogger log)
    {
        try
        {
            var history = await GetStockHistory(ticker, fromDate, toDate);
            if (skipNews)
                return new { ticker, history, news = (object?)null };
            if (RateLimitMode) await Task.Delay(12000);
            var news = await GetStockNews(ticker);
            return new { ticker, history, news };
        }
        catch (Exception ex)
        {
            log.LogError($"Error fetching {ticker}: {ex.Message}");
            return new { ticker, error = ex.Message };
        }
    }

    private async Task<object> GetStockHistory(string ticker, string fromDate, string toDate)
    {
        var url = $"{BaseUrl}/v2/aggs/ticker/{ticker}/range/1/day/{fromDate}/{toDate}?adjusted=true&sort=asc&limit=50000&apiKey={ApiKey}";
        var response = await client.GetStringAsync(url);
        var json = JObject.Parse(response);

        return json["results"]?.Select(r => new
        {
            date = DateTimeOffset.FromUnixTimeMilliseconds((long)r["t"]!).ToString("yyyy-MM-dd"),
            open = r["o"]?.ToObject<decimal>(),
            high = r["h"]?.ToObject<decimal>(),
            low = r["l"]?.ToObject<decimal>(),
            close = r["c"]?.ToObject<decimal>(),
            volume = r["v"]?.ToObject<long>()
        }).ToList()!;
    }

    private async Task<object> GetStockNews(string ticker)
    {
        var url = $"{BaseUrl}/v2/reference/news?ticker={ticker}&limit=5&sort=published_utc&apiKey={ApiKey}";
        var response = await client.GetStringAsync(url);
        var json = JObject.Parse(response);

        return json["results"]?.Select(a => new
        {
            title = a["title"]?.ToString(),
            published = a["published_utc"]?.ToString(),
            source = a["publisher"]?["name"]?.ToString(),
            url = a["article_url"]?.ToString(),
            sentiment = a["insights"]?[0]?["sentiment"]?.ToString()
        }).ToList()!;
    }
}