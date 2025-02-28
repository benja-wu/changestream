# changestream
Java MongoDB changestream repo based on SpringBoot framework

## Design
1. **Resumable**. It will automatically store every resume token during business logic processing and resume changestream listener using saved token when it starts. **Note:** Since it can't be guarantee that every resume token can be stored successfully(VM crashed? network partition? ), this framework will use the earestly resume token among all threads in that last round. So multiple events(related to the number of threads) will be delivered twice(each player's events keep order). Please make sure your event processing logic is **idempotent**. You can use the user case below as a reference. 
2. **AutoRetry**. It has configurable auto-retry logic during the event handling, for MongoDB Java driver, **Network Exceptions**, **Transient Errors**, and **Server Selection Errors** are retied autumnally by itself. Others exceptions, such as  MongoTimeoutException | MongoSocketReadException | MongoSocketWriteException | MongoCommandException | MongoWriteConcernException need to handle manually. 
3. **Multiple Threads**. It supports multiple threads execution with configurable thread numbers. 
4. **Extensibility**. This demo has two business logic handler in `src/main/java/com/example/demo/service/impl/task1.java` and `src/main/java/com/example/demo/service/impl/task2.java`folder, task1 implements one complicated pipeline, and task2 only display the event. Can reuse demo as example to add your new customized business logic. 
5. **Observability**. It exposes TPS/P99 latency/Total request numbers metrics with Prometheus library and HTTP endpoint.

## User case
In `src/main/java/com/example/demo/service/impl/task1.java`,  it watches the source collection, user's new transaction doc will be inserted as below:
``` bash
{"playerID":1003,"transactionID": 100003, "name":"ben","date":ISODate(), "value":23.1})
```
We need to update this transaction into target collection's doc 
``` bash
{
    _id: ObjectId('66f4e4cdb1b0f322afb766a8'),
    playerID: 1003,
    gamingDate: ISODate('2024-09-26T00:00:00.000Z'),
    name: 'ben',
    txns: [
      {
        transactionID: 100002,
        value: 20.1,
        date: ISODate('2024-09-26T04:39:27.379Z')
      },
      {
        transactionID: 100004,
        value: 63.1,
        date: ISODate('2024-09-26T04:57:23.595Z')
      },
      {
        transactionID: 100003,
        value: 23.1,
        date: ISODate('2024-09-26T04:57:45.787Z')
      }
    ],
    lastModified: ISODate('2024-09-26T04:57:45.798Z')
  }

```
1. One play will generate one doc per day, use playerID+ gamingDate as the daily target document filter.
2. Match the player's daily one transaction with target collections' 'txns''s elements by 'transactionID' fields, if the the transaction exists, replace the element with change steam event. If not, append it int to the 'txns' array field. 
3. Single mongoDB command solution
``` bash

db.userdailytxn.updateOne(
  { playerID: 1003, gamingDate: ISODate('2024-09-26T00:00:00.000Z') }, // Find document by playerID and gamingDate
  [
    {
      $set: {
        playerID: "$$ROOT.playerID",
        gamingDate: "$$ROOT.gamingDate",
        name: { $ifNull: ["$name", "ben"] }, // Set 'name' when upserting
        txns: {
          $let: {
            vars: {
              newTxn: { transactionID: 100004, value: 33.1, date: ISODate() }, // Define the new transaction to be added or updated
              existingTxn: {
                $first: {
                  $filter: {
                    input: "$txns",
                    cond: { $eq: ["$$this.transactionID", 100004] }, // Check for the existence of the new transactionID
                  },
                },
              },
            },
            in: {
              $cond: {
                if: { $not: ["$$existingTxn"] }, // If the transaction does not exist
                then: {
                  $concatArrays: [
                    { $ifNull: ["$txns", []] }, // Initialize txns array if not present
                    ["$$newTxn"], // Add the new transaction
                  ],
                },
                else: {
                  $map: {
                    input: "$txns",
                    as: "txn",
                    in: {
                      $cond: {
                        if: { $eq: ["$$txn.transactionID", 100004] }, // Match to replace only when transactionID matches
                        then: "$$newTxn", // Replace with the new transaction values
                        else: "$$txn", // Keep other transactions as is
                      },
                    },
                  },
                },
              },
            },
          },
        },
        lastModified: ISODate(), // Update the last modified date whenever the document is modified
      },
    },
  ],
  {
    upsert: true, 
    setOnInsert: { playerID: 1003, gamingDate: ISODate('2024-09-26T00:00:00.000Z'), name: 'ben' }, // Ensure these fields are set on insert
  }
);

```

### Event sequence design
1. Due to the multiple thread processing, one user's event may be consumed with different thread, that may cause the order violation for the some user's event.
2. This framework will distributed event into the same thread by using one hash method as below:
```java
     int playerID = fullDocument.getInteger("playerID");
     // Determine which executor to use based on playerID
     int executorIndex = playerID % nums;

     LOGGER.info("evnet {}, playerID {}, executor index {}", event, playerID, executorIndex);
     // Submit the task to the corresponding executor
     CompletableFuture.runAsync(() -> processEvent(event), executors[executorIndex])
```

## Observability
1. Use Premethues libiary, expose related metris for observability 
2. Metrics includs
```bash
% curl http://localhost:8081/metrics
# HELP event_lag_per_thread Real-time event lag per thread.
# TYPE event_lag_per_thread gauge
event_lag_per_thread{thread_name="Thread-0",} 31099.0
# HELP tps_per_thread TPS as exponentially-weighted moving average in last 15 minutes per thread.
# TYPE tps_per_thread gauge
tps_per_thread{thread_name="Thread-0",} 3.222222222222222E-4
# HELP event_process_duration_seconds Histogram for tracking event processing duration.
# TYPE event_process_duration_seconds histogram
event_process_duration_seconds_bucket{le="0.0",} 0.0
event_process_duration_seconds_bucket{le="0.05",} 2.0
event_process_duration_seconds_bucket{le="0.1",} 2.0
event_process_duration_seconds_bucket{le="0.2",} 2.0
event_process_duration_seconds_bucket{le="0.5",} 2.0
event_process_duration_seconds_bucket{le="0.7",} 2.0
event_process_duration_seconds_bucket{le="1.0",} 2.0
event_process_duration_seconds_bucket{le="2.0",} 2.0
event_process_duration_seconds_bucket{le="+Inf",} 2.0
event_process_duration_seconds_count 2.0
event_process_duration_seconds_sum 0.032
# HELP total_events_handled_successfully_total Total number of successful events handled across all threads.
# TYPE total_events_handled_successfully_total counter
total_events_handled_successfully_total 2.0
# HELP total_events_handled_total Total number of events handled across all threads.
# TYPE total_events_handled_total counter
total_events_handled_total 2.0
# HELP p99_processing_time_milliseconds Processing time for 99% of the requests in milliseconds.
# TYPE p99_processing_time_milliseconds summary
p99_processing_time_milliseconds{quantile="0.99",} 23.0
p99_processing_time_milliseconds_count 2.0
p99_processing_time_milliseconds_sum 32.0
# HELP event_process_duration_seconds_created Histogram for tracking event processing duration.
# TYPE event_process_duration_seconds_created gauge
event_process_duration_seconds_created 1.728355414493E9
# HELP p99_processing_time_milliseconds_created Processing time for 99% of the requests in milliseconds.
# TYPE p99_processing_time_milliseconds_created gauge
p99_processing_time_milliseconds_created 1.728355414495E9
# HELP total_events_handled_created Total number of events handled across all threads.
# TYPE total_events_handled_created gauge
total_events_handled_created 1.72835541449E9
# HELP total_events_handled_successfully_created Total number of successful events handled across all threads.
# TYPE total_events_handled_successfully_created gauge
total_events_handled_successfully_created 1.728355414492E9

```
