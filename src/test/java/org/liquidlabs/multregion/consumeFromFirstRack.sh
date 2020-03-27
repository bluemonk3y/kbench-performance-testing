#!/bin/bash -v
echo "stuff"

set args = "--topicName perf-topic-1"
java org.liquidlabs.multiregion.ConsumeFromRack $args

