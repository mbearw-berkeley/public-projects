# Video Game Event Logging

In this situation, I am a data scientist at Nintendo working on The Legend Of Zelda: Breath of the Wild 2 and I'm interested in tracking the types of items users buy in the game. I'll be completing the following tasks:

## Tasks
-Instrument API server to log events to Kafka

-Assemble a data pipeline to catch these events: use Spark streaming to filter select event types from Kafka, land them into HDFS/parquet to make them available for analysis using Presto

-Use Apache Bench to generate test data for pipeline

-Produce an analytics report with a description of the pipeline and some basic analysis of the events
