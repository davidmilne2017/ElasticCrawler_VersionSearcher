using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;

namespace ElasticCrawler
{
    public class SqlRepository
    {
        private const string InsertApiRequest = @"insert into api_request (controller, url, headers, request_body, app_user_id, install_guid,
                        update_timestamp) 
            values (@Controller, @Url, @Headers, @RequestBody, @AppUserId, @InstallGuid, @UpdateInsertTimestamp);";
        public static async Task InsertData(string connectionString, IEnumerable<ApiRequest> requests)
        {

            try
            {
                await using var con = new SqlConnection(connectionString);
                await con.ExecuteAsync("truncate table api_request");
                await con.ExecuteAsync(InsertApiRequest, requests);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}