# Airflow, PySpark, Iceberg and DuckDB: build local environment with AI (almost) 

This is attempt to create working project with AI for pySpark with Airflow and Iceberg.
Initially, asked Cursor.ai (with Claude) to create sample project with different options using latest Airflow 3.x and Spark 4.x.
With option to run with minIO instead of s3 storage.

You can see [cursor_init_chat.md](cursor_init_chat.md). The structure looked nice and initial code at average level, but nothing worked
despite "tests" cursor claimed it passed. After few attempts, tried to talk with Gemini ([gemini.md](gemini.md)) - improved few things, but still failed to get something running.
Then tried to get help from chatGpt ([chatGpt_airflow3.md](chatGpt_airflow3.md)).

Then with help of reading documentation, refactoring by myself, and very specific/concrete questions to both chatGpt and Gemini, 
I finally succeeded to run app and to submit it spark master.

Funny things: 
* all 3 AIs suggested some mixed configuration for airflow 3.x (the latest) and previous 2.x. So airflow failed to start.
* all 3 AIs tried to put configuration for spark 4 and Iceberg, but only direct question to Gemini (after I found in documentation) if Iceberg works with Spark 4, and it answered that still in work

So downgraded spark to 3.5.6.

Finally, despite getting few wrong suggestions from Gemini and ChatGPT about right spark configurations, 
succeeded to run airflow, to show data and to write it to minIO.

Finally, simple job with Iceberg works. Good for me :).

As bonus, I added simple DAG using __DuckDB__ without any spark.

For more details, you can read my blog article :) [Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2)
<a href="https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2" target="_new">
![Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*yRVmIRJ5TTJu7KKaQoc_JA.jpeg "Airflow, PySpark, Iceberg: build local environment with AI (almost)")
</a>