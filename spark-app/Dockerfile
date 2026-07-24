FROM apache/spark:3.5.1

USER root

RUN python3 -m pip install --no-cache-dir \
    cassandra-driver redis kafka-python \
    numpy==2.4.4 joblib==1.5.3 scikit-learn==1.8.0 \
    pandas==3.0.3 xgboost==3.2.0 lightgbm==4.5.0

WORKDIR /opt/project

COPY fraud_pipeline /opt/project/fraud_pipeline
COPY model /opt/project/model
COPY spark-app /opt/project/spark-app-build

ENV PYTHONPATH=/opt/project:/opt/spark/app

RUN sed -i 's/\r$//' /opt/project/spark-app-build/entrypoint.sh \
    && chmod +x /opt/project/spark-app-build/entrypoint.sh

CMD ["/opt/project/spark-app-build/entrypoint.sh"]



