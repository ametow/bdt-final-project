FROM python:3.11-slim

RUN pip install --no-cache-dir happybase==1.2.0 thriftpy2==0.6.0

COPY init_tables.py /init_tables.py

ENTRYPOINT ["python3", "/init_tables.py"]
