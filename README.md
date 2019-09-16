# Cosmos MongoDB Read Write Sample

1. Please update the app.config with connection string
2. Please increase the RUs for collection if you want to ingest data quickly.

Notes:

1. This is a guiding sample and doesn't have the code for handling failed insertion docs and read docs after certain number of retries. Please look at the code to increase the retires for failed operation in case of Request rate Large error.

2. Use DocsCount and BatchSize to adjust the numbers threads parallel can write to Cosmos Mongo DB. The same applies to Read scenario also.

3. Request Rate Large error is a signal sends by server to client requesting to lower the number of requests submitting as it is exceeding the provisioned RUs . In this sample this was handled using sleep. Each thread will for few milliseconds with in the range of (MinWait, MaxWait).
