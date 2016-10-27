#!/bin/bash
spark-submit \
	--class ca.redsofa.jobs.SimpleBatch \
	--master local[*] \
  	./target/batch_job-1.0-SNAPSHOT.jar
