#!/usr/bin/env bash
set -e
exec /opt/spark/bin/spark-submit ${SPARK_CONF} /opt/spark/app/batch_job.py