# realtime-streaming
Realtime Streaming application using Active MQ, Spark streaming (Java), Elasticsearch and Kibana

In this setup we are removing duplicating records from streaming as well as batch records.

Active MQ (Persistence Layer) --> Spark Stremaing (Procesing Engine) --> Elasticsearch (Cache or Storage) --> (Analytics Engine)
