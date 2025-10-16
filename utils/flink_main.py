import json, os ,datetime
from pyflink.datastream import StreamExecutionEnvironment, CheckpointConfig, ExternalizedCheckpointCleanup
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common import Configuration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream.connectors import DeliveryGuarantee
from utils.configuration import Config
from flink_steps.flow_manager import FlowManager
from flink_steps.flink_step import StepError
from utils.logger import Logger
import socket
import json
from typing import Dict, Any
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink,
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common import Time
from dataclasses import dataclass

from typing import Optional, Dict, Any






class DataFlinkJob:

    def __init__(self):
        # ✅ Setting configuration

        # path to the Python in your devcontainer venv or volume
        PY = "/home/vscode/.venv/bin/python"   # or "/venv/bin/python" if you used a volume
        os.environ["PYFLINK_CLIENT_EXECUTABLE"] = PY
        os.environ["PYFLINK_EXECUTABLE"] = PY

        self.config = Configuration()
        self.config.set_string("classloader.resolve-order", "parent-first")
        self.config.set_string("python.client.executable", PY)
        self.config.set_string("python.executable", PY)
        # Where to store checkpoints (filesystem path inside your container/cluster)
        # Use a durable path in real clusters (HDFS/S3/GCS/ABFS, etc.)
        self.config.set_string("state.checkpoints.dir", "file:///workspaces/data_etl_flinkjob/tmp/flink-checkpoints")
        config_file_path = 'configurations/configuration.yml'
        data_config = Config(config_file_path).get_config()
        logger = Logger(configuration=Config(config_file_path))
        DataFlinkJob.flow_manager = FlowManager(config=data_config,logger=logger)

        # ✅ Initialize Flink environment
        self.env = StreamExecutionEnvironment.get_execution_environment(configuration=self.config)
        self.env.set_parallelism(data_config['parallelism'])
        self.env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
            3,  # number of restart attempts
            10000  # delay in milliseconds
        ))
        # --- Checkpointing (Exactly-Once) ---
        self.env.enable_checkpointing(10_000)  # every 10s; tune to your SLA/load
        cfg = self.env.get_checkpoint_config()
        cfg.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
        cfg.set_min_pause_between_checkpoints(5_000)   # was Time.seconds(5)
        cfg.set_checkpoint_timeout(120_000)            # was Time.minutes(2)
        cfg.set_max_concurrent_checkpoints(1)                       # simpler w/ Kafka TX
        cfg.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        #no watermark
        self.wm = WatermarkStrategy.no_watermarks()
        

        # Define JAR Paths
        JAR_DIR = data_config['jars']['JAR_DIR']

        # Convert to correct URI format
        FLINK_KAFKA_JAR = "file:///workspaces/data_etl_flinkjob/jars/flink-connector-kafka-3.4.0-1.20.jar"
        KAFKA_CLIENTS_JAR = "file:///workspaces/data_etl_flinkjob/jars/kafka-clients-3.9.0.jar"

        # ✅ Wire JARs
        self.env.add_jars(FLINK_KAFKA_JAR,KAFKA_CLIENTS_JAR)

        # Kafka Configuration
        INPUT_TOPICS = data_config['kafka']['INPUT_TOPICS']
        OUTPUT_TOPIC = data_config['kafka']['OUTPUT_TOPIC']
        ERROR_TOPIC = data_config['kafka']['ERROR_TOPIC']


        input_kafka_properties = data_config['kafka']['input_kafka_properties']
        input_kafka_properties['client.id'] = socket.gethostname()
        output_kafka_properties = data_config['kafka']['output_kafka_properties']
        error_kafka_properties = data_config['kafka']['error_kafka_properties']

        # ✅ Kafka Source (Flink Native)
        self.kafka_sources=[]
        for part in data_config['kafka']['parts']:
            kafka_source = self.json_source(part["topic"],part["bootstrap"]).map(lambda r: {
                "kind": part["name"],
                "parent_key": part["key_field"],
                "row": r
            })
            self.kafka_sources.append(kafka_source)
        unioned = self.kafka_sources[0]
        for i in range(1, len(self.kafka_sources)):
            unioned = unioned.union(self.kafka_sources[i])
        # Key by order_id and merge into one per-key state object
        result_stream = unioned.key_by(lambda e: e["parent_key"]).process(MergeAll(data_config))


        # ✅ Kafka Sink (Flink Native)
        self.kafka_sink = self.build_string_kafka_sink(error_kafka_properties,OUTPUT_TOPIC)

        # ✅ Kafka Sink (Flink Native)
        self.kafka_sink_errors = self.build_string_kafka_sink(error_kafka_properties,ERROR_TOPIC)
        
        # Step 1: Process the message and mark errors
        tagged_stream = result_stream.map(
            DataFlinkJob.process_message,
            output_type=Types.STRING()
        )

        # Step 2: Convert to (is_error: bool, data: str)
        routed_stream = tagged_stream.map(
            DataFlinkJob.parse_and_route,
            output_type=Types.TUPLE([Types.BOOLEAN(), Types.STRING()])
        )

        # Step 3: Split into success and error streams
        success_stream = routed_stream \
            .filter(lambda t: not t[0]) \
            .map(lambda t: t[1], output_type=Types.STRING())

        error_stream = routed_stream \
            .filter(lambda t: t[0]) \
            .map(lambda t: t[1], output_type=Types.STRING())

        # Step 4: Sink each stream
        success_stream.sink_to(self.kafka_sink)
        error_stream.sink_to(self.kafka_sink_errors)

        
        # ✅ Execute the Flink Job
        self.env.execute("Kafka Streaming with Flink Kafka Connector")

    # ✅ Data Transformation
    @staticmethod
    def parse_and_route(json_str):
        record = json.loads(json_str)
        return record["is_error"], record["data"]

    @staticmethod
    def process_message(value):
        try:
            config_file_path = 'configurations/configuration.yml'
            if not hasattr(DataFlinkJob, "logger"):
                DataFlinkJob.logger = Logger(configuration=Config(config_file_path))

            logger = DataFlinkJob.logger
            data = json.loads(value)
            payload = data

            res = DataFlinkJob.flow_manager.execute_flow([data], payload)

            if res:
                result = json.dumps(DataFlinkJob.flow_manager.msg)
            else:
                data["src.payload"] = payload
                data["flink_msg"] = {}
                logger.insert_info_to_log('process_message', data)
                result = json.dumps(data)
            return json.dumps({"data": result, "is_error": False})

        except StepError as e:
            if not hasattr(DataFlinkJob, "logger"):
                DataFlinkJob.logger = Logger(configuration=Config(config_file_path))
            logger.insert_error_to_log("process_message", e.error_code,e.error_type,e.msg)
            return json.dumps({"data": json.dumps(e.msg), "is_error": True})


        except Exception as e:
            if not hasattr(DataFlinkJob, "logger"):
                DataFlinkJob.logger = Logger(configuration=Config(config_file_path))
            error_type = type(e).__name__
            error_code = DataFlinkJob.logger.get_error_code(error_type)
            error_message = {
                "error": str(e),
                "error_type": error_type,
                "error_code": error_code,
                "payload": value
            }
            logger.insert_error_to_log("process_message", error_code,error_type,error_message)
            return json.dumps({"data": json.dumps(error_message), "is_error": True})
        
    
  # Builders  
    def json_source(self,topic: str,bootstrap: str):
        src = (
            KafkaSource.builder()
            .set_bootstrap_servers(bootstrap)
            .set_topics(topic)
            .set_group_id("lab-enricher-ds")
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        return self.env.from_source(src, self.wm, f"src-{topic}").map(lambda s: json.loads(s))


    def build_string_kafka_sink(self, props: dict, topic: str) -> KafkaSink:
        """
        Optional helper to build a string KafkaSink using the same properties dict.
        Only bootstrap.servers is required for the sink; the rest are client params.
        """
        bs = props["bootstrap.servers"]
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(bs)
            .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .set_property("transaction.timeout.ms", "600000")
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
    

@dataclass
class LabAgg:
    parent_key: str
    data: Optional[Dict[str, Any]] = None    # all parts
    last_version: Optional[str] = None        # hash/version to avoid dup emits

    def is_ready(self) -> bool:
        res = True
        for item in self.parts:
            if self.data[item["source.table"]] is None:
                res = False
        return res

    def as_enriched(self) -> Optional[Dict[str, Any]]:
        if not self.is_ready():
            return None
        o = self.order or {}
        t = self.test or {}
        c = self.code or {}
        return {
            "patient_id": t.get("patient_id") or o.get("patient_id"),
            "order_id": self.order_id,
            "lab_test_id": t.get("lab_test_id"),
            "test_code": t.get("test_code") or c.get("test_code"),
            "test_name": c.get("name"),
            "loinc": c.get("loinc"),
            "result_value": _to_float(t.get("result_value")),
            "units": t.get("units") or c.get("units"),
            "ref_low": _to_float(c.get("ref_low")),
            "ref_high": _to_float(c.get("ref_high")),
            "abnormal_flag": t.get("abnormal_flag"),
            "status": t.get("status") or o.get("order_status"),
            "priority": o.get("priority"),
            "collected_at": t.get("collected_at"),
            "resulted_at": t.get("resulted_at"),
            "updated_at": t.get("updated_at") or o.get("updated_at"),
            "lab_site": t.get("lab_site"),
            "event_ts_ms": t.get("__ts_ms") or o.get("__ts_ms") or c.get("__ts_ms"),
            "provenance": {"tables": ["LAB_TEST" if self.test else None,
                                      "LAB_ORDER" if self.order else None,
                                      "LAB_CODE" if self.code else None]}
        }

def _to_float(v):
    try: return None if v in (None, "") else float(v)
    except: return None


class MergeAll(KeyedProcessFunction):
    def __init__(self,data_config):
        super().__init__()
        self.data_config = data_config

    def open(self, ctx: RuntimeContext):
        ttl = StateTtlConfig.new_builder(Time.minutes(5)).cleanup_full_snapshot().build()
        desc = ValueStateDescriptor("agg", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(ttl)
        self.agg_state = ctx.get_state(desc)

    def _get_or_init(self, parent_key: str) -> LabAgg:
        v = self.agg_state.value()
        return v if v is not None else LabAgg(parent_key=parent_key)
    
    def _get_parent_key(self, kiend, tagged) -> str:
        key_field = tagged["row"]
        for part in self.data_config['kafka']['parts']:
            if part["name"]==kiend:
                key_field =  part["key_field"]
                arr = str(key_field).split(".")
                for item in arr:
                    key_field = key_field[item]
        return key_field           

    def process_element(self, tagged,out):
        kiend = (tagged.get("kined") or "").strip()
        parent_key = self._get_parent_key(kiend,tagged)
        if not parent_key:
            return
        agg = self._get_or_init(parent_key)
        row = tagged["row"]
        kind = tagged["kind"]

        # apply change
        if kind == "ORDER":
            agg.order = None if is_delete(row) else row
        elif kind == "TEST":
            agg.test  = None if is_delete(row) else row
        elif kind == "CODE":
            agg.code  = None if is_delete(row) else row

        # persist new state
        self.agg_state.update(agg)

        # emit if ready and changed
        enriched = agg.as_enriched()
        if enriched is not None:
            vhash = self._stable_hash(enriched)
            if vhash != agg.last_version:
                agg.last_version = vhash
                self.agg_state.update(agg)
                out.collect(enriched)

        # optional: register a cleanup timer to guard leaks for stragglers
        # ctx.timer_service().register_processing_time_timer(ctx.timer_service().current_processing_time() + 48*3600*1000)

    def _stable_hash(self, obj) -> str:
        # hash without volatile fields
        masked = dict(obj)
        masked["event_ts_ms"] = None
        s = json.dumps(masked, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(s.encode("utf-8")).hexdigest()
