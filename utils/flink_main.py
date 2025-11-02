# utils/flink_main.py

import os
import json
import socket
import hashlib
import uuid
from dataclasses import dataclass
from typing import Optional, Dict, Any

from pathlib import Path
from dotenv import load_dotenv, find_dotenv

from pyflink.common import Configuration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies

from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    CheckpointConfig,
    ExternalizedCheckpointCleanup,
)
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common import Time

from configuration import Config
from logger import Logger


# ========================
# Runtime / Env bootstrap
# ========================
# Finds the nearest .env walking up from current file/cwd
load_dotenv(find_dotenv())   # or load_dotenv(Path(__file__).with_name(".env"))
# path to Python in your devcontainer venv
PY = os.getenv("PY", "/home/vscode/.venv/bin/python")  # default fallback
APP_ENV   = os.getenv("APP_ENV", "production")  # default fallback
DEBUG     = os.getenv("DEBUG", "false").lower() == "true"
PORT      = int(os.getenv("PORT", "8000"))
SECRET    = os.getenv("SECRET_KEY")             # keep out of source control
STATE_CHECKPOINTS_DIR =  os.getenv("STATE_CHECKPOINTS_DIR")
CONFIG_FILE_PATH = os.getenv("CONFIG_FILE_PATH")
PARALLELISM = int(os.getenv("PARALLELISM"))
OUTPUT_TOPIC   = os.getenv("OUTPUT_TOPIC")  
ERROR_TOPIC   = os.getenv("ERROR_TOPIC") 
AUDIT_TOPIC   = os.getenv("AUDIT_TOPIC")

# Load YAML configuration
main_config = Config(CONFIG_FILE_PATH)
data_config = main_config.get_config()


os.environ["PYFLINK_CLIENT_EXECUTABLE"] = PY
os.environ["PYFLINK_EXECUTABLE"] = PY

config = Configuration()
config.set_string("classloader.resolve-order", "parent-first")
config.set_string("python.client.executable", PY)
config.set_string("python.executable", PY)
# Local FS for checkpoints in dev; use durable storage in real clusters
config.set_string("state.checkpoints.dir", STATE_CHECKPOINTS_DIR)

# Make Python operators flush small, frequent bundles (helps checkpointing)
config.set_string("python.fn-execution.bundle.size", "100")
config.set_string("python.fn-execution.bundle.time", "100")  # ms

env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
env.set_parallelism(data_config.get("parallelism", PARALLELISM))
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10_000))

# --- Checkpointing (dev-friendly, robust) ---
env.enable_checkpointing(30_000)  # every 30s
chk: CheckpointConfig = env.get_checkpoint_config()
chk.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
chk.set_min_pause_between_checkpoints(30_000)
chk.set_checkpoint_timeout(600_000)  # 10 minutes
chk.set_max_concurrent_checkpoints(1)
chk.enable_unaligned_checkpoints(True)
chk.set_tolerable_checkpoint_failure_number(10)
chk.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

# --- Watermarks ---
wm = WatermarkStrategy.no_watermarks()

# --- Add connector JARs (if needed; harmless if already on classpath) ---
FLINK_KAFKA_JAR = os.getenv("FLINK_KAFKA_JAR")
KAFKA_CLIENTS_JAR = os.getenv("KAFKA_CLIENTS_JAR")
env.add_jars(FLINK_KAFKA_JAR, KAFKA_CLIENTS_JAR)

input_kafka_properties = data_config["kafka"]["input_kafka_properties"]
input_kafka_properties["client.id"] = socket.gethostname()
output_kafka_properties = data_config["kafka"]["output_kafka_properties"]


# ========================
# Helpers & Builders
# ========================

def json_source(env: StreamExecutionEnvironment, topic: str, bootstrap: str):
    """Create a Kafka source that outputs Python dicts parsed from JSON strings."""
    src = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id("lab-enricher-ds")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return env.from_source(src, wm, f"src-{topic}").map(lambda s: json.loads(s))


def build_string_kafka_sink(kafka_props: Dict[str, Any], topic: str) -> KafkaSink:
    """KafkaSink that expects **String** values (JSON text)."""
    bootstrap = kafka_props.get("bootstrap.servers")
    record_ser = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())  # Java String
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(record_ser)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)  # simpler for dev
        .build()
    )


def _dig(obj: Dict[str, Any], path: str):
    """Safely get nested value by dot path, returns None if any segment missing."""
    cur = obj
    for seg in str(path).split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(seg)
        else:
            cur = getattr(cur, seg, None)
    return cur


def make_key_extractor(cfg: Dict[str, Any]):
    """Returns a function that extracts parent_key from a tagged record using cfg."""
    parts_cfg = cfg["kafka"]["parts"]

    def extract(e: Dict[str, Any]):
        kind = e.get("kind")
        row = e.get("row", {})
        for part in parts_cfg:
            if part.get("name") == kind:
                return _dig(row, part.get("key_field"))
        return None

    return extract


# ========================
# State model + Operator
# ========================

@dataclass
class LabAgg:
    parent_key: str
    message_id: str = None
    data: Optional[Dict[str, Any]] = None      # raw data divided by kind
    parts: Optional[Dict[str, Any]] = None     # parts dict from configuration
    last_version: Optional[str] = None         # hash/version to avoid dup emits

    def is_ready(self) -> bool:
        if not self.parts or self.data is None:
            return False
        for item in self.parts:
            if item["name"] not in self.data:
                return False
        return True

    def as_enriched(self) -> Optional[Dict[str, Any]]:
        return self.data if self.is_ready() else None


class MergeAll(KeyedProcessFunction):
    def __init__(self, data_config):
        super().__init__()
        self.data_config = data_config

    def open(self, ctx: RuntimeContext):
        ttl = StateTtlConfig.new_builder(Time.minutes(5)).cleanup_full_snapshot().build()
        desc = ValueStateDescriptor("agg", Types.PICKLED_BYTE_ARRAY())
        desc.enable_time_to_live(ttl)
        self.agg_state = ctx.get_state(desc)
        self.logger = Logger()

    # ------- helpers -------
    def _get_or_init(self, parent_key: str) -> LabAgg:
        v = self.agg_state.value()
        return v if v is not None else LabAgg(parent_key=parent_key, parts=self.data_config["kafka"]["parts"])

    def _stable_hash(self, obj) -> str:
        masked = dict(obj)
        if "event_ts_ms" in masked:  # ignore volatile fields if present
            masked["event_ts_ms"] = None
        s = json.dumps(masked, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(s.encode("utf-8")).hexdigest()

    # ------- main operator (2-arg; emit via yield) -------
    def process_element(self, tagged, ctx):
        """
        tagged: {"kind": "<PART_NAME>", "row": {...}, "parent_key": <optional>}
        Emits: enriched dicts (yield)
        """
        self.logger.insert_debug_to_log("DMergeAll.process_element","enter function")
        kind = (tagged.get("kind") or "").strip()
        if not kind:
            return

        # Prefer precomputed parent_key if present; otherwise recompute
        parent_key = tagged.get("parent_key")
        if not parent_key:
            extractor = make_key_extractor(self.data_config)
            parent_key = extractor(tagged)
        if not parent_key:
            # Debug: show why we dropped it
            self.logger.insert_info_to_log("MergeAll.process_element",f"[PROCESS] drop: missing key for kind={kind}")
            return

        agg = self._get_or_init(parent_key)
        row = tagged.get("row", {})

        if getattr(agg, "data", None) is None:
            agg.data = {}
            agg.message_id = uuid.uuid4()

        # merge this part
        agg.data[kind] = row

        # persist state
        self.agg_state.update(agg)

        # build enriched and de-dup BEFORE emitting
        enriched = agg.as_enriched()
        if enriched is None:
            self.logger.insert_info_to_log("MergeAll.process_element",f"[PROCESS] not ready key={parent_key}, have={list(agg.data.keys())}")
            return

        vhash = self._stable_hash(enriched)
        if vhash != getattr(agg, "last_version", None):
            agg.last_version = vhash
            self.agg_state.update(agg)
            self.logger.insert_info_to_log("MergeAll.process_element",f"[PROCESS] EMIT key={parent_key}")
            yield enriched
        else:
            # unchanged; skip emit
            pass


# ========================
# Graph wiring
# ========================
def main(env):
    # Build per-topic sources → tagged union
    kafka_sources = []
    logger = Logger()
    logger.insert_debug_to_log("main","enter function")
    for part in data_config["kafka"]["parts"]:
        topic = part["topic"]
        bootstrap = part["bootstrap"]
        name = part["name"]

        src = json_source(env, topic, bootstrap).map(
            # bind name to default arg so lambda captures its value
            lambda r, _name=name: {"kind": _name, "row": r},
            output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        )
        kafka_sources.append(src)

    if not kafka_sources:
        raise RuntimeError("No Kafka parts configured in configuration.yml")

    # Union all sources
    unioned = kafka_sources[0]
    for i in range(1, len(kafka_sources)):
        unioned = unioned.union(kafka_sources[i])

    # Attach parent_key to each record (so keyBy doesn't rebuild it per record)
    extract_parent_key = make_key_extractor(data_config)
    with_key = (
        unioned
        .map(lambda e: {**e, "parent_key": extract_parent_key(e)},
            output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))
        .filter(lambda e: e["parent_key"] is not None)
    )

    # Key by parent_key and merge into one per-key state object
    result_stream = with_key.key_by(lambda e: e["parent_key"]).process(
        MergeAll(data_config),
        # Type bridge for Python operator results across the JVM boundary
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    # Sink: dict -> JSON string (ensure Java String for SimpleStringSchema)
    kafka_sink = build_string_kafka_sink(output_kafka_properties, OUTPUT_TOPIC)
    (
        result_stream
        .map(lambda d: json.dumps(d, ensure_ascii=False), output_type=Types.STRING())
        .sink_to(kafka_sink)
        .name("enriched-json-out")
    )
    logger.insert_debug_to_log("main","execute flink environment")
    env.execute("Kafka Streaming with Flink Kafka Connector")
    logger.insert_debug_to_log("main","end function")

# Execute
if __name__ == "__main__":
    main(env)
    
