FROM apache/spark:3.5.8-python3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
    pip install --no-cache-dir happybase==1.2.0 thriftpy2==0.6.0 && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tmp/.ivy2 && chown -R spark:spark /tmp/.ivy2

USER spark
WORKDIR /app
