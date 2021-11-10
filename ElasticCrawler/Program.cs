using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Dasync.Collections;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Web.Http.Controllers;
using Nest;
using drivitMainModel.DataObjects;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Configuration;

namespace ElasticCrawler
{
    class Program
    {
        
        private const string CloudContainer = "payload";
        private const string ElasticIndex = "newapilog";
        
        static void Main(string[] args)
        {
            
            var config = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .AddUserSecrets<Program>()
                .Build();

            var node = new Uri(config.GetConnectionString("ElasticUrl"));
            var settings = new ConnectionSettings(node)
                .DefaultFieldNameInferrer(p => p)
                .EnableDebugMode();
            var client = new ElasticClient(settings);
            
            var newQuery = new QueryContainer();
            newQuery &= Query<NewApiLog>.DateRange(
                            d =>
                                d.Field("UpdateInsertTimeStamp").GreaterThanOrEquals(new DateTime(2021, 9 ,1)))
                        && (Query<NewApiLog>.Match(c => c.Field("Controller").Query("AppEventArray"))
                        || (Query<NewApiLog>.Match(c => c.Field("Controller").Query("Auto"))
                            && Query<NewApiLog>.Match(c => c.Field("URL").Query("ElectricModelsPerClass")))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("Beat"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("ChargingEvents"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("ConnectionAttempt"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("ConsumptionData"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("FuelBrand"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("FuelDiscount"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("FuelType"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("StationService"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("TecDoc2"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("TecDoc"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("Triggers"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("UnstructuredTecDoc2"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("UnstructuredTecDoc"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("UserInfoV2"))
                        || Query<NewApiLog>.Match(c => c.Field("Controller").Query("Voltage"))
                        );

            var logs = new List<NewApiLogWithPayload>();
            var response = client.Search<NewApiLogWithPayload>(s => s
                .Size(10000)
                    .Index(ElasticIndex)
                    .Query(q => newQuery)
                .Scroll("1000m"));
            
            if (response.Documents.Any())
                logs.AddRange(response.Documents);

            do
            {
                var result = response;
                response = client.Scroll<NewApiLogWithPayload>("1000m", result.ScrollId);
                logs.AddRange(response.Documents);
            //} while (response.IsValid && response.Documents.Any() && logs.Count <= 10000);
            } while (response.IsValid && response.Documents.Any());

            var controllers = logs.GroupBy(x => x.Controller);
            foreach (var controller in controllers)
            {
                Console.WriteLine(controller.Key);
            }

            ParseRequest(logs).GetAwaiter().GetResult();

            // DownloadBlobs(config.GetConnectionString("StorageAuth"), 
            //     logs.Where(x => x.HasPayload()).ToList()).GetAwaiter().GetResult();
            // SqlRepository.InsertData(config.GetConnectionString("CommonDb"),
            //     logs.Select(x => x.MapToApiRequest())).GetAwaiter().GetResult();

        }

        public static async Task ParseRequest(List<NewApiLogWithPayload> logs)
        {

            var requestDictionary = new ConcurrentDictionary<string, NewApiLogWithPayload>();
            
            await logs.ParallelForEachAsync(async log =>
            {
                var headers = log.Headers.Split(
                    new[] { "\r\n", "\r", "\n" },
                    StringSplitOptions.None
                );
                var headerVersion = headers.FirstOrDefault(x => x.ToLower().StartsWith("drivit-version"));
                OperatingSystem os;
                if (log.Headers.ToLower().Contains("android"))
                    os = OperatingSystem.Android;
                else if (log.Headers.ToLower().Contains("ios"))
                    os = OperatingSystem.Ios;
                else
                {
                    Console.WriteLine($"Couldn't determine Os from {log.Headers}");
                    return;
                }
                
                if (!string.IsNullOrEmpty(headerVersion))
                {
                    log.Version = headerVersion.Split(":")[1];
                    var urlController = "";
                    var urlParts = log.URL.Split("/");
                    if (urlParts.Length >= 2)
                        urlController = $"{urlParts[^2]}/{urlParts[^1]}";
                    if (urlController.Contains("?"))
                        urlController = urlController.Split("?")[0];

                    var key = $"{urlController}_{os.ToString()}";

                    if (!requestDictionary.ContainsKey(key))
                    {
                        requestDictionary.TryAdd(key, log);
                    }
                    else if (string.CompareOrdinal(requestDictionary[key].Version, log.Version) < 0)
                    {
                        requestDictionary[key] = log;
                    }
                }
            }, 50);

            foreach (var (key, value) in requestDictionary)
                Console.WriteLine($"{key} : {value.Version}");
            
        }

        private static async Task DownloadBlobs(string connectionString, List<NewApiLogWithPayload> logs)
        {

            await logs.ParallelForEachAsync(async log =>
            {
                var ms = await DownloadToStreamAsync(connectionString, CloudContainer,
                    log.RequestPayloadGuid + ".json");
                log.RequestPayload = Encoding.ASCII.GetString(ms.ToArray());
            }, 50);
        }
        
        private static async Task<MemoryStream> DownloadToStreamAsync(string connectionString, string bucketName, string fileName)
        {
            var blobClient = new BlockBlobClient(connectionString, bucketName, fileName);
            var ms = new MemoryStream();
            await blobClient.DownloadToAsync(ms);
            return ms;
        }
        
    }
}