# Install python env packages
pip install -r requirements.txt

sudo mkdir -p /opt/flink/lib
# Flink 1.20 Kafka connector (DataStream)
sudo cp -r /workspaces/data_etl_flinkjob/jars/flink-connector-kafka-3.4.0-1.20.jar \
  /opt/flink/lib/flink-connector-kafka-3.4.0-1.20.jar
# Your Kafka client override
sudo cp -r /workspaces/data_etl_flinkjob/jars/kafka-clients-3.9.0.jar \
  /opt/flink/lib/kafka-clients-3.9.0.jar

ls -h /opt/flink/lib

pysp=$(python -c 'import site; print([p for p in site.getsitepackages() if p.endswith("site-packages")][0])')
sudo mkdir -p "$pysp/pyflink/lib"
sudo cp /opt/flink/lib/kafka-clients-3.9.0.jar \
        /opt/flink/lib/flink-connector-kafka-3.4.0-1.20.jar "$pysp/pyflink/lib/"

sudo mkdir -p /workspaces/data_etl_flinkjob/.flink/{conf,logs}
sudo cat > /workspaces/data_etl_flinkjob/.flink/conf/flink-conf.yaml <<'YAML'
# put logs in the workspace (avoids permission errors)
env.log.dir: /workspaces/data_etl_flinkjob/.flink/logs

# Open JDK modules needed by PyFlink & your earlier classpath edits
env.java.opts.client: "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"
env.java.opts.taskmanager: "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"
env.java.opts.jobmanager: "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"
YAML

