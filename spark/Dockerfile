FROM apache/spark:3.5.0

WORKDIR /app

COPY transform_commodities_stream.py .

ENTRYPOINT ["/opt/spark/bin/spark-submit", "transform_commodities_stream.py"]
