### What


Collects JMX stats using Jolokia REST calls and dumps them to ./out/date/<metric>.out

### Why

Provides a simple way of capturing JMX metrics for measuring broker load

### What do I do with the data?

Import it into Elasticsearch of something else that can plot the JSON data.

### What mbeans do I need to track?

* Log flush latency (95th percentile)
* Under-replicated partitions
* Messages in / sec per broker and per topic
* Bytes in / sec per broker (collected as a system metric from each broker using collectd)
* Bytes in / sec per topic
* Bytes / message

Otherwise 1:

kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent
OneMinuteRate<brt>

kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent

kafka.controller:type=KafkaController,name=ActiveControllerCount

kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions

kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
 
kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
 
kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce/FetchConsumer/FetchFollower}
 
kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec
 
kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs

 
kafka.controller:type=KafkaController,name=OfflinePartitionsCount
 
kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs
 
kafka.server:type=ReplicaManager,name=PartitionCount

 
kafka.server:type=ReplicaManager,name=IsrShrinksPerSec
 
kafka.network:type=RequestMetrics,name=TotalTimeMs,request={Produce/FetchConsumer/FetchFollower}
 
kafka.network:type=RequestMetrics,name=RequestQueueTimeMs,request={Produce/FetchConsumer/FetchFollower}
 
kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs,request={Produce/FetchConsumer/FetchFollower}
 
kafka.network:type=RequestMetrics,name=LocalTimeMs,request={Produce/FetchConsumer/FetchFollower}

 
kafka.network:type=RequestMetrics,name=RemoteTimeMs,request={Produce/FetchConsumer/FetchFollower}
 
kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request={Produce/FetchConsumer/FetchFollower}
 
kafka.server:type=ZooKeeperClientMetrics,name=ZooKeeperRequestLatencyMs

## Otherwise:

   **Heap** java.lang:type=Memory/HeapMemoryUsage/used

   **Thread Count**
      mbean  = "java.lang:type=Threading/TotalStartedThreadCount"
     paths= ["ThreadCount","DaemonThreadCount","PeakThreadCount"]

   **Garbage Collection**
      mbean  = "java.lang:type=GarbageCollector,name=*"
     paths = ["CollectionCount","CollectionTime"]

   **Topic Metrics**
      mbean  = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"<br>     
      mbean  = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec"<br>
      mbean  = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec"<br>
      mbean  = "kafka.server:name=RequestHandlerAvgIdlePercent,type=KafkaRequestHandlerPool"

   **Kafka Server Delayed Operation Purgatory Metrics**
     mbean  = "kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Fetch"<br>
     mbean  = "kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Produce"<br>

   **Kafka Server Replica Fetcher Manager Metrics**
     mbean  = "kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica"<br>

   **Kafka Server Replica Manager Metrics**
     mbean  = "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"<br>
     mbean  = "kafka.server:type=ReplicaManager,name=PartitionCount"<br>
     mbean  = "kafka.server:type=ReplicaManager,name=LeaderCount"<br>
     mbean  = "kafka.server:type=ReplicaManager,name=IsrShrinksPerSec"<br>
     mbean  = "kafka.server:type=ReplicaManager,name=IsrExpandsPerSec"<br>

   **Kafka Network Request Metrics**
     mbean  = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchConsumer"<br>
     mbean  = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchFollower"<br>
     mbean  = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce"<br>
     mbean  = "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer"<br>
     mbean  = "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower"<br>
     mbean  = "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce"<br>

   **Kafka Network Processor Metrics**
     mbean  = "kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer"<br>

   **Kafka Controller Metrics**
     mbean  = "kafka.controller:type=KafkaController,name=ActiveControllerCount"<br>
     mbean  = "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"<br>
     mbean  = "kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs"<br>
     mbean  = "kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec"<br>

   **Zookeeper Metrics**
     mbean  = "kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec"<br>
     paths = ["Count","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","MeanRate"]<br><br>     
     mbean  = "kafka.server:type=SessionExpireListener,name=ZooKeeperSyncConnectsPerSec"<br>
     paths = ["Count","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","MeanRate"]<br><br>
     mbean  = "kafka.server:type=SessionExpireListener,name=ZooKeeperAuthFailuresPerSec"<br>
     paths = ["Count","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","MeanRate"]<br><br>
     mbean  = "kafka.server:type=SessionExpireListener,name=ZooKeeperReadOnlyConnectsPerSec"<br>
     paths = ["Count","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","MeanRate"]<br><br>
     mbean  = "kafka.server:type=SessionExpireListener,name=ZooKeeperSaslAuthenticationsPerSec"<br>
     paths = ["Count","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","MeanRate"]<br><br>
     mbean  = "kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec"<br>
     paths = ["Count","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","MeanRate"]<br><br>