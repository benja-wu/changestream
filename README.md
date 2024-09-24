# changestream
Java MongoDB changestream repo based on SpringBoot framework

## Design
1. It's resumeable. It will store every resume token during business logic handling automattly 
2. It has configurable autoretry logic during the event handling, for MongoDB Java driver, **Network Exceptions**, **Transient Errors**, and **Server Selection Errors** are retied automally by itself. Others exceptions, such as  MongoTimeoutException | MongoSocketReadException | MongoSocketWriteException | MongoCommandException | MongoWriteConcernException need to handle manully. 
3. It supports multple threads execution with configurable thread numbers. 
4. It watches one collection's change event only. If we need to watch multiple collections in MongoDB, start different instances with different configurations. 