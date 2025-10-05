# Spark 4.0.1 + Python 3
FROM apache/spark:4.0.1-python3

# Work as root briefly to prepare writable dirs
USER root

ARG SPARK_UID=185

# Create a real home and Ivy cache dir, make them writable by the runtime user
ENV SPARK_HOME=/opt/spark
ENV SPARK_WORKDIR=/opt/spark/work-dir
RUN mkdir -p ${SPARK_WORKDIR}/.ivy2 \
 && chown -R ${SPARK_UID}:0 ${SPARK_WORKDIR} \
 && chmod -R 775 ${SPARK_WORKDIR}

# Force Java and Spark to use our dirs for dependency resolution
# (covers the submit launcher, driver, and executors)
ENV SPARK_SUBMIT_OPTS="-Duser.home=${SPARK_WORKDIR} -Divy.home=${SPARK_WORKDIR} -Dspark.jars.ivy=${SPARK_WORKDIR}/.ivy2"

# (Optional) Python deps
COPY pyproject.toml  ./
# Optional: if you have a LICENSE or other files referenced by the backend, copy them too
# Upgrade pip and install your project (this reads [project.dependencies])
RUN python3 -m pip install --no-cache-dir --upgrade pip \
 && python3 -m pip install --no-cache-dir .

# App files
RUN mkdir -p /opt/app/src /opt/app/jars
COPY spark/jars/ /opt/app/jars/
COPY src/ /opt/app/src/

# Drop back to the non-root user used by the Spark image
USER ${SPARK_UID}

# Use --packages OR --jars. Prefer --packages for Kafka.
CMD ["/opt/spark/bin/spark-submit", \
     "--conf", "spark.jars.ivy=/opt/spark/work-dir/.ivy2", \
     "--conf", "spark.driver.extraJavaOptions=-Duser.home=/opt/spark/work-dir -Divy.home=/opt/spark/work-dir", \
     "--conf", "spark.executor.extraJavaOptions=-Duser.home=/opt/spark/work-dir -Divy.home=/opt/spark/work-dir", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1", \
     "/opt/app/src/main.py"]
