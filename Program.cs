namespace CosmosMongoDBAggregationSample
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.Security.Authentication;
    using System.Text;
    using MongoDB.Bson;
    using MongoDB.Driver;

    class Program
    {
        private static MongoClient mongoClient;
        private static string dbName = "axasrini";
        private static string collectionName = "event";
        private static IMongoDatabase database;
        private static IMongoCollection<BsonDocument> aggCollection;


        static void ExecuteAggregationQuery(DateRange dr)
        {
            FilterDefinitionBuilder<BsonDocument>
            matchFilterDefinition = Builders<BsonDocument>.Filter;
            var filter = matchFilterDefinition.Eq("event_type", "Address_Contact_Change_Request_Event") &
            matchFilterDefinition.Gt("request_status_isodate", dr.Begin.ToString("o", CultureInfo.InvariantCulture)) &
            matchFilterDefinition.Lt("request_status_isodate", dr.End.ToString("o", CultureInfo.InvariantCulture));

            var matchFilter = BsonDocument.Parse("{\"event_type\":\"Address_Contact_Change_Request_Event\",\"request_status_isodate\":{$ne:null}}");
            var sortFilter= BsonDocument.Parse("{\"request_status_isodate\":-1}");
            var groupByFilter= BsonDocument.Parse("{\"_id\":{\"agreement_id\":\"$agreement_id\"},\"events\":{ \"$push\":{ \"event\":\"$$CURRENT\"} }}");
            var projectOp = BsonDocument.Parse("{\"events\":{\"$slice\":[\"$events\",1]}}");
            var finalProjectOp=BsonDocument.Parse("{\"_id\":0,\"data\":\"$events.event\"}");
            var docs= aggCollection
                .Aggregate()
                .Match(filter)
                .Sort(sortFilter)
                .Group(groupByFilter)
                .Project(projectOp)
                .Unwind("events")
                .Project(finalProjectOp)
                .ToList();

        }

        static void PrintDateRanges(List<DateRange> dateRanges)
        {
            foreach(DateRange dr in dateRanges)
            {
                Console.WriteLine("Start date time:{0} - End date time {1} - interval in mins {2}", dr.Begin, dr.End, dr.End.Subtract(dr.Begin).TotalMinutes);
            }
        }

        static void Main(string[] args)
        {
            string connectionString =
               ConfigurationManager.AppSettings["conn"];

            MongoClientSettings settings = MongoClientSettings.FromUrl(
                new MongoUrl(connectionString)
            );
            settings.SslSettings =
                new SslSettings() { EnabledSslProtocols = SslProtocols.Tls12 };
            settings.MaxConnectionPoolSize = 6000;
            mongoClient = new MongoClient(settings);

            database = mongoClient.GetDatabase(dbName);

            aggCollection = database.GetCollection<BsonDocument>(collectionName);

            // Please run this if you want to insert fresh data.
            //GenerateSampleDocuments();
            TestAggregation();

            Console.WriteLine("Press enter to exit...");

            Console.ReadLine();
        }

        private static void TestAggregation()
        {
            DateTime endDatetime = DateTime.UtcNow;

            // Please note that you need to subtract more minutes if the data is too old. 
            DateTime startDateTime = endDatetime.Subtract(new TimeSpan(0, 0, 30, 0));
            List<DateRange> dateRanges = DateUtil.GenerateRanges(startDateTime, endDatetime, 0, 0, 1);
            //PrintDateRanges(dateRanges);
            foreach(DateRange dr in dateRanges)
            {
                ExecuteAggregationQuery(dr);
            }
        }

        private static string RandomString(int size, bool lowerCase)
        {
            StringBuilder builder = new StringBuilder();
            Random random = new Random();
            char ch;
            for (int i = 1; i < size + 1; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                builder.Append(ch);
            }
            if (lowerCase)
                return builder.ToString().ToLower();
            else
                return builder.ToString();
        }
        private static void GenerateSampleDocuments()
        {
            // Total documents
            int aggrements = 5;
            int repetations = 2;

            DateTime startDateTime = DateTime.UtcNow.Subtract(new TimeSpan(0, aggrements * repetations, 0));

            for (int agreementId=1; agreementId<=aggrements; agreementId++)
            {

                for (int j = 0; j < repetations; j++)
                {
                    StringBuilder sr = new StringBuilder();
                    sr.Append("{");
                    sr.Append("\"event\" : \""); sr.Append(RandomString(4, true)); sr.Append("\",");
                    sr.Append("\"event_type\" : \""); sr.Append("Address_Contact_Change_Request_Event"); sr.Append("\",");
                    sr.Append("\"event_id\" : "); sr.Append(agreementId); sr.Append(",");
                    sr.Append("\"agreement_id\" : "); sr.Append(agreementId); sr.Append(",");
                    sr.Append("\"request_status_isodate\" : \""); sr.Append(startDateTime.ToString("o", CultureInfo.InvariantCulture)); sr.Append("\"");
                    sr.Append("}");
                    startDateTime=startDateTime.AddMinutes(1);
                    BsonDocument doc = BsonDocument.Parse(sr.ToString());
                    aggCollection.InsertOne(doc);
                }
            }

        }
    }
}
