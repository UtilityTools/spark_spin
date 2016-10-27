#!/bin/bash
spark-submit \
	--class ca.redsofa.jobs.StructuredStreamingAverage \
	--master local[*] \
  	 ./target/stream_avg-1.0-SNAPSHOT.jar