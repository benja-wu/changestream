# changestream
Java MongoDB changestream repo based on SpringBoot framework

## Design
1. **Resumable**. This framework will automatically store every resume token during business logic processing and resume changestream listener using saved token when it starts. **Note:** Since it can't be guarantee that every resume token can be stored successfully(VM crashed, network partition...), this framework will use the earestly resume token among all threads in that last round. So multiple events(related to the number of threads) will be delivered twice. Please ensure your event processing logic is **idempotent**. You can use the user case below as a reference. 
2. **AutoRetry**. This framework relies MongoDB Java driver for auto-retry logic during the network interruption secnarios, e.g., **Network Exceptions**, **Transient Errors**, and **Server Selection Errors**. They can be retryied by driver autumnally. Can checkout mongodb auto retry spec for other exceptions, such as  MongoTimeoutException | MongoSocketReadException | MongoSocketWriteException | MongoCommandException | MongoWriteConcernException. 
3. **Concurrency event handling**. This framework supports multiple threads execution concurrencyly. As default, it create one thread for listeing collection events and use executor to handle event asychasynchronously. 
4. **Single instance for multiple collection listening**. This framework supports listeing multiple collection and allocated dedicated thread pool for each collection.  
5. **Extensibility**. This demo has multiple business logic handlers  in `src/main/java/com/example/demo/service/impl/` folder. Can use it as references. 
6. **Observability**. It exposes TPS/P99 latency/Total request numbers metrics with Prometheus library and HTTP endpoint.

## User case
### Case 1 update with pipeline
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

#### Event sequence design
1. Due to the multiple thread processing, one user's event may be consumed with different thread, that may cause the order violation for the some user's event.
2. This framework doesn't guarantee this event distribution consistency now. To implement it, consider using one business field for routing and change `executors` to hold single thread per executors in one collection. Then route to the dedicated executor as the business filed mod exectuor number in one collection. 

### Case 2 multiple collections merge calculation 

Changestream Input
* Existing 8 collections: Awards, Stub, Promo, Points,PromotionRedemption,  PromotionRuleOutCome, PrizeLocnMapping 
 
Changestream triggers
1. Awards collection on data insert/update
2. PlayerStub collection on data insert/update
3. PlayerPromo collection on data insert/update
4. PlayerPoints collection on data insert/update
5. PromotionRedeemtion collection on data insert/update, use PromotionRedeemtion.PlayerID and PromotionRedeemtion.PrizeID to find match Awards, and then use Awards.tranId to find related Stub, Promo and Points collection docs. 
 

Changestream Output
* One new collection, member_awards 


In `src/main/java/com/example/demo/service/impl/AwardCalculationService.java`, `src/main/java/com/example/demo/service/impl/Awards.java`,`src/main/java/com/example/demo/service/impl/Points.java`,`src/main/java/com/example/demo/service/impl/Promo.java`, `src/main/java/com/example/demo/service/impl/Stub.java`, and `src/main/java/com/example/demo/service/impl/PromotionRedemption.java` files, we receive the corresponding collections' change stream event and merge all related fileds into one ouput collections, member_awards 

`AwardCalculationService.java` implements the fields calculation logic, here is one final output doc sample 

Sample ouput doc 
```json
{
  "_id": {
    "$oid": "67d13015cec0ee139583e534"
  },
  "created_dtm": {
    "$date": "2022-09-29T07:18:44.580Z"
  },
  "promotion_id": null,
  "player_session_id": 1,
  "player_type_id": 100000001,
  "modified_by": 777777777,
  "tran_id": {
    "$numberLong": "10025445779"
  },
  "dept_id": 100000004,
  "trip_id": {
    "$numberLong": "10625389"
  },
  "auth_emp_id": 777777777,
  "group_id": 100000001,
  "locn_id": 110003536,
  "prize_qty": 1,
  "casino_id": 110000002,
  "gaming_dt": {
    "$date": "2022-09-29T00:00:00.000Z"
  },
  "auth_award": {
    "$numberDecimal": "99998.0000"
  },
  "void_tran_id": null,
  "is_open_item": true,
  "post_dtm": {
    "$date": "2022-09-29T07:18:43.740Z"
  },
  "void_emp_id": null,
  "_ods_is_deleted": false,
  "game_id": 100000001,
  "shift": 2,
  "player_id": 777777777,
  "created_by": 777777777,
  "denom_id": 100000002,
  "allowed_purge_dt": {
    "$date": "2022-09-29T00:00:00.000Z"
  },
  "server_work_station": "server3",
  "computer_name": "qa-dt012345",
  "ref1": null,
  "award_code": "O",
  "ref2": "",
  "emp_id": 777777777,
  "aging_dt": {
    "$date": "2022-09-29T00:00:00.000Z"
  },
  "item_code": "A",
  "site_id": 533,
  "data_row_version": 1,
  "rep_id": {
    "$numberLong": "0"
  },
  "_ods_deleted_dtm": null,
  "_ods_replay_switch": false,
  "prize_id": 1,
  "outlet": null,
  "_ods_modified_dtm": {
    "$date": "2024-12-23T07:42:39.922Z"
  },
  "sequence_id": 0,
  "_ods_created_dtm": {
    "$date": "2024-12-23T07:42:39.922Z"
  },
  "award_used": {
    "$numberDecimal": "99999.0000"
  },
  "related_tran_id": {
    "$numberLong": "10025445779"
  },
  "tran_code_id": 8,
  "document_no": "",
  "old_related_tran_id": {
    "$numberLong": "10025445779"
  },
  "is_distributed": false,
  "area_id": 110000519,
  "modified_dtm": {
    "$date": "2022-09-29T07:18:44.580Z"
  },
  "flags": 0,
  "unique_code_receiving_id": null,
  "is_void": false,
  "trip_type": "N",
  "void_auth_emp_id": null,
  "trip_dt": {
    "$date": "2022-07-05T00:00:00.000Z"
  },
  "_ods_is_archived": false,
  "player_stub": {
    "player_id": 777777777,
    "tran_code_id": 8,
    "redeem_stubs": 0,
    "expire_stubs": 0,
    "partial_stubs": {
      "$numberDecimal": "99999.0000"
    },
    "bucket_group_id": 3,
    "modified_by": 777777777,
    "tran_id": {
      "$numberLong": "10025445779"
    },
    "partial_stubs2": {
      "$numberDecimal": "99999.0000"
    },
    "base_stubs": 0,
    "_ods_deleted_dtm": null,
    "_ods_replay_switch": false,
    "gaming_dt": {
      "$date": "2024-10-22T00:00:00.000Z"
    },
    "adj_stubs_dr": 0,
    "adj_stubs_cr": 1,
    "bonus_stubs": 0,
    "stubs_bal": 0,
    "_ods_is_deleted": false,
    "_ods_modified_dtm": {
      "$date": "2025-02-28T03:03:14.358Z"
    },
    "_ods_created_dtm": {
      "$date": "2025-02-28T03:03:14.358Z"
    }
  },
  "player_promo1": {
    "expiry_date": {
      "$date": "2024-07-15T00:00:00.000Z"
    },
    "expire_promo1": {
      "$numberDecimal": "99999.0000"
    },
    "over_promo1": {
      "$numberDecimal": "99999.0000"
    },
    "bucket_group_id": 4,
    "modified_by": 25,
    "tran_id": {
      "$numberLong": "10025445779"
    },
    "adj_promo1_dr": {
      "$numberDecimal": "99999.0000"
    },
    "_ods_deleted_dtm": {
      "$date": "2024-12-24T18:01:05.434Z"
    },
    "_ods_replay_switch": false,
    "gaming_dt": {
      "$date": "2024-07-08T00:00:00.000Z"
    },
    "promo1": {
      "$numberDecimal": "99999.0000"
    },
    "_ods_is_deleted": true,
    "_ods_modified_dtm": {
      "$date": "2024-12-24T18:01:05.434Z"
    },
    "_ods_created_dtm": {
      "$date": "2024-12-23T07:56:27.987Z"
    },
    "player_id": 777777777,
    "adj_promo1_cr": {
      "$numberDecimal": "99999.0000"
    },
    "tran_code_id": 8,
    "promo1_used": {
      "$numberDecimal": "0.0000"
    },
    "bonus_promo1": {
      "$numberDecimal": "99999.0000"
    },
    "promo1_bal": {
      "$numberDecimal": "99999.0000"
    },
    "_ods_is_archived": false
  },
  "player_points": [
    {
      "expiry_date": {
        "$date": "2025-03-06T00:00:00.000Z"
      },
      "partial_pt2_overflow": false,
      "partial_pt1_overflow": false,
      "qual_pts": 0,
      "bucket_group_id": 2,
      "bonus_pts": 0,
      "modified_by": 777777777,
      "tran_id": {
        "$numberLong": "10025445779"
      },
      "tran_code_id": 8,
      "adj_pts_dr": 0,
      "_ods_deleted_dtm": null,
      "_ods_replay_switch": false,
      "gaming_dt": {
        "$date": "2024-05-21T00:00:00.000Z"
      },
      "base_pts": 0,
      "_ods_is_deleted": false,
      "_ods_modified_dtm": {
        "$date": "2024-12-23T07:53:34.124Z"
      },
      "redeem_pts": 880,
      "_ods_created_dtm": {
        "$date": "2024-12-23T07:53:34.124Z"
      },
      "player_id": 777777777,
      "pts_bal": 49362,
      "expire_pts": 0,
      "adj_pts_cr": 0,
      "game_pts": 0,
      "over_pts": 0,
      "_ods_is_archived": false,
      "partial_pts2": {
        "$numberDecimal": "99999.0000"
      },
      "partial_pts": {
        "$numberDecimal": "99999.0000"
      }
    },
    {
      "expiry_date": {
        "$date": "2025-06-20T00:00:00.000Z"
      },
      "partial_pt2_overflow": false,
      "partial_pt1_overflow": false,
      "qual_pts": 100,
      "bucket_group_id": 3,
      "bonus_pts": 50,
      "modified_by": 777777777,
      "tran_id": {
        "$numberLong": "10025445779"
      },
      "tran_code_id": 8,
      "adj_pts_dr": 5,
      "_ods_deleted_dtm": null,
      "_ods_replay_switch": false,
      "gaming_dt": {
        "$date": "2024-06-15T00:00:00.000Z"
      },
      "base_pts": 200,
      "_ods_is_deleted": false,
      "_ods_modified_dtm": {
        "$date": "2024-12-24T10:15:30.245Z"
      },
      "redeem_pts": 500,
      "_ods_created_dtm": {
        "$date": "2024-12-24T10:15:30.245Z"
      },
      "player_id": 777777777,
      "pts_bal": 30500,
      "expire_pts": 10,
      "adj_pts_cr": 20,
      "game_pts": 30,
      "over_pts": 40,
      "_ods_is_archived": false,
      "partial_pts2": {
        "$numberDecimal": "88888.0000"
      },
      "partial_pts": {
        "$numberDecimal": "77777.0000"
      }
    }
  ],
  "prize": {
    "award_code": "A",
    "prize_code": "ABCDEFG",
    "prize_name": "ABCDEFG",
    "prize_id": 1
  },
  "prize_locn_mapping": {
    "locn_id": 110001528,
    "casino_id": 110000002,
    "locn_code": "GLPCage"
  },
  "award_prize_type": -1,
  "is_doc_pmprize": false
}
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
