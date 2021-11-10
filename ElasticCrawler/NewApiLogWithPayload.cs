using drivitMainModel.DataObjects;

namespace ElasticCrawler
{
    public class NewApiLogWithPayload : NewApiLog
    {
        public string RequestPayload { get; set; }
        public string Version { get; set; }
        public OperatingSystem OperatingSystem { get; set; }
    }

    public enum OperatingSystem
    {
        Android,
        Ios
    }
    

    public static class ApiExtensions
    {
        public static bool HasPayload(this NewApiLogWithPayload apiLog) =>
            !string.IsNullOrEmpty(apiLog.RequestPayloadGuid.ToString());

        public static ApiRequest MapToApiRequest(this NewApiLogWithPayload apiLog) =>
            new ()
            {
                Controller = apiLog.Controller,
                Url = apiLog.URL,
                Headers = apiLog.Headers,
                RequestBody = apiLog.RequestPayload,
                AppUserId = apiLog.AppUserId,
                InstallGuid = apiLog.InstallGuid,
                UpdateInsertTimeStamp = apiLog.UpdateInsertTimeStamp
            };
    }
    
}