# changestream
Java MongoDB changestream repo based on SpringBoot framework

## Design
1. It's resumeable. It will store every resume token during business logic handling automattly 
2. It has configurable autoretry logic during the event handling, for MongoDB Java driver, **Network Exceptions**, **Transient Errors**, and **Server Selection Errors** are retied automally by itself. Others exceptions, such as  MongoTimeoutException | MongoSocketReadException | MongoSocketWriteException | MongoCommandException | MongoWriteConcernException need to handle manully. 
3. It supports multple threads execution with configurable thread numbers. 
4. It watches one collection's change event only. If we need to watch multiple collections in MongoDB, start different instances with different configurations. 

## Observability
1. Use Premethues libiary, expose related metris for observability 
2. Metrics includs
```bash
java_test % curl http://localhost:8081/metrics
# HELP p99_processing_time_milliseconds Processing time for 99% of the requests in milliseconds.
# TYPE p99_processing_time_milliseconds summary
p99_processing_time_milliseconds{quantile="0.99",} 18.0
p99_processing_time_milliseconds_count 3.0
p99_processing_time_milliseconds_sum 33.0
# HELP total_events_handled_total Total number of events handled across all threads.
# TYPE total_events_handled_total counter
total_events_handled_total 0.0
# HELP event_lag_per_thread Real-time event lag per thread.
# TYPE event_lag_per_thread gauge
event_lag_per_thread{thread_name="Thread-4",} 12.0
event_lag_per_thread{thread_name="Thread-5",} 12.0
event_lag_per_thread{thread_name="Thread-6",} 8.0
# HELP total_events_handled_successfully_total Total number of successful events handled across all threads.
# TYPE total_events_handled_successfully_total counter
total_events_handled_successfully_total 0.0
# HELP tps_per_thread TPS as exponentially-weighted moving average in last 15 minutes per thread.
# TYPE tps_per_thread gauge
tps_per_thread{thread_name="Thread-4",} 1.1111111111111112E-4
tps_per_thread{thread_name="Thread-5",} 1.1111111111111112E-4
tps_per_thread{thread_name="Thread-6",} 1.1111111111111112E-4
# HELP event_process_duration_seconds Histogram for tracking event processing duration.
# TYPE event_process_duration_seconds histogram
event_process_duration_seconds_bucket{le="0.0",} 0.0
event_process_duration_seconds_bucket{le="0.05",} 3.0
event_process_duration_seconds_bucket{le="0.1",} 3.0
event_process_duration_seconds_bucket{le="0.2",} 3.0
event_process_duration_seconds_bucket{le="0.5",} 3.0
event_process_duration_seconds_bucket{le="0.7",} 3.0
event_process_duration_seconds_bucket{le="1.0",} 3.0
event_process_duration_seconds_bucket{le="2.0",} 3.0
event_process_duration_seconds_bucket{le="+Inf",} 3.0
event_process_duration_seconds_count 3.0
event_process_duration_seconds_sum 0.033
# HELP event_process_duration_seconds_created Histogram for tracking event processing duration.
# TYPE event_process_duration_seconds_created gauge
event_process_duration_seconds_created 1.727188426406E9
# HELP p99_processing_time_milliseconds_created Processing time for 99% of the requests in milliseconds.
# TYPE p99_processing_time_milliseconds_created gauge
p99_processing_time_milliseconds_created 1.727188426408E9
# HELP total_events_handled_created Total number of events handled across all threads.
# TYPE total_events_handled_created gauge
total_events_handled_created 1.727188426403E9
# HELP total_events_handled_successfully_created Total number of successful events handled across all threads.
# TYPE total_events_handled_successfully_created gauge
total_events_handled_successfully_created 1.727188426404E9

```