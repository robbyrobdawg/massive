using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public class StockDataFunction
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string ApiKey = Environment.GetEnvironmentVariable("MASSIVE_API_KEY") ?? "";
    private static readonly string BaseUrl = "https://api.polygon.io";

    private static readonly List<string> DefaultTickers = new List<string>
    {
        "AAPL", "MSFT", "VOO", "VTI"
    };

    [Function("GetStockData")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "stock/{ticker?}")] 
        HttpRequestData req,
        string? ticker,
        FunctionContext context)
    {
        var log = context.GetLogger("StockDataFunction");
        log.LogInformation("Fetching stock data");

        var tickers = string.IsNullOrEmpty(ticker)
            ? DefaultTickers
            : new List<string> { ticker.ToUpper() };

        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var fromDate = query["from"] ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");
        var toDate = query["to"] ?? DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd");

        var tasks = tickers.Select(t => GetTickerData(t, fromDate, toDate, log));
        var results = await Task.WhenAll(tasks);

        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new
        {
            timestamp = DateTime.UtcNow,
            from = fromDate,
            to = toDate,
            data = results
        });

        return response;
    }

    private async Task<object> GetTickerData(string ticker, string fromDate, string toDate, ILogger log)
    {
        try
        {
            var priceTask = GetStockHistory(ticker, fromDate, toDate);
            var newsTask = GetStockNews(ticker);
            await Task.WhenAll(priceTask, newsTask);

            return new
            {
                ticker,
                history = priceTask.Result,
                news = newsTask.Result
            };
        }
        catch (Exception ex)
        {
            log.LogError($"Error fetching {ticker}: {ex.Message}");
            return new { ticker, error = ex.Message };
        }
    }

    private async Task<object> GetStockHistory(string ticker, string fromDate, string toDate)
    {
        var url = $"{BaseUrl}/v2/aggs/ticker/{ticker}/range/1/day/{fromDate}/{toDate}?adjusted=true&sort=asc&apiKey={ApiKey}";
        var response = await client.GetStringAsync(url);
        var json = JObject.Parse(response);

        return json["results"]?.Select(r => new
        {
            date = DateTimeOffset.FromUnixTimeMilliseconds((long)r["t"]!).ToString("yyyy-MM-dd"),
            open = r["o"],
            high = r["h"],
            low = r["l"],
            close = r["c"],
            volume = r["v"]
        }).ToList()!;
    }

    private async Task<object> GetStockNews(string ticker)
    {
        var url = $"{BaseUrl}/v2/reference/news?ticker={ticker}&limit=5&sort=published_utc&apiKey={ApiKey}";
        var response = await client.GetStringAsync(url);
        var json = JObject.Parse(response);

        return json["results"]?.Select(a => new
        {
            title = a["title"],
            published = a["published_utc"],
            source = a["publisher"]?["name"],
            url = a["article_url"],
            sentiment = a["insights"]?[0]?["sentiment"]
        }).ToList()!;
    }
}