- Hit a bug ([3] Unknown Topic Or Partition) which was caused by kafka-go's client not requesting auto-creation of the topic by default
  - Solution: Explicitly turn AllowAutoTopicCreation: true to the kafka.Writer struct in main.go

- Read error: Go sent a constant string "hello_kafka" to Kafka, but if this run crashes, it may read an old attempt's data and print "OK"
  - Solution: Send a unique string every time

- kafka.Writer is async, and if a network timeout happens, the write reports "Failure" to Go, but internal cleanup loop makes the message down the pipe anyway
  - Solution: Switch to a synchronous kafka.Conn, which either finishes before 10 second deadline or kills connection imemdiately

- When producer sends a message to a topic which isn't created yet, Kafka tries to create it on the fly (auto creation), but the first few messaes will bounce off and fail
  - Solution: Use conn.CreateTopics to create the topic manually beforehand to avoid race condition

- If connection fails while closing, having defer conn.Close() will not log this
  - Solution: Use closeQuietly to print errors when network connection is dropped

- Write error: When the produce reaches the broker successfully, an acknowledgement packet is sent back to Go to say it succeeded, but if this packet fails, then Go would never know if it succeeded and would send the message again
  - Solution: Remove the retry loop and use synchronous kafka.Conn for same reason as point 3 since there is no way to know to tell apart if something already happened v.s. just hearing back
