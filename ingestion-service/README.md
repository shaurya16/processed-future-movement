# ingestion-service

Spring Boot service that reads the System A fixed-width file (`Input.txt`) using the
parser in [`common`](../common/) and publishes one Kafka event per transaction record
to the `future-transactions` topic.

Stands in for the "Kafka streaming input instead of file" scenario called out in the
requirements — this service is the producer side of that pipeline.
