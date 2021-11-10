using System;

namespace ElasticCrawler
{
    public class ApiRequest
    {
        public string Controller { get; set; }
        public string Url { get; set; }
        public string Headers { get; set; }
        public string RequestBody { get; set; }
        public string AppUserId { get; set; }
        public string InstallGuid { get; set; }
        public DateTime UpdateInsertTimeStamp { get; set; }
    }
    
}