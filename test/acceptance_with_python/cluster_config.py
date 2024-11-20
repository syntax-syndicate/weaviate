from dataclasses import dataclass
from enum import Enum
from typing import Dict, List
import tempfile
import yaml


class ClusterType(Enum):
    SINGLE_NODE = "single"
    THREE_NODE = "three"


class InstanceType(Enum):
    REGULAR = "regular"
    RBAC = "rbac"


@dataclass
class NodeConfig:
    http_port: int
    grpc_port: int
    hostname: str
    environment: Dict[str, str]


class WeaviateClusterConfig:
    def __init__(self, cluster_type: ClusterType, instance_type: InstanceType):
        self.cluster_type = cluster_type
        self.instance_type = instance_type
        self.nodes: List[NodeConfig] = []
        self._setup_nodes()

    def _setup_nodes(self):
        base_port = 8080
        base_grpc_port = 50051
        base_env = {
            "LOG_LEVEL": "debug",
            "QUERY_DEFAULTS_LIMIT": "20",
            "PERSISTENCE_DATA_PATH": (
                "./data-auth" if self.instance_type == InstanceType.RBAC else "./data"
            ),
            "BACKUP_FILESYSTEM_PATH": "/var/lib/backups",
            "DISABLE_TELEMETRY": "true",
            "DISABLE_RECOVERY_ON_PANIC": "true",
            "QUERY_MAXIMUM_RESULTS": "10005",
        }

        if self.instance_type == InstanceType.REGULAR:
            base_env.update(
                {
                    "CONTEXTIONARY_URL": "contextionary:9999",
                    "DEFAULT_VECTORIZER_MODULE": "text2vec-contextionary",
                    "ENABLE_MODULES": "text2vec-contextionary,backup-filesystem,generative-dummy,reranker-dummy",
                    "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
                    "ASYNC_INDEXING": "false",
                    "PROMETHEUS_MONITORING_ENABLED": "true",
                    "PROMETHEUS_MONITORING_GROUP_CLASSES": "true",
                    "PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS": "2",
                }
            )
        else:
            base_env.update(
                {
                    "ENABLE_MODULES": "backup-filesystem,generative-dummy,reranker-dummy",
                    "AUTHORIZATION_ENABLE_RBAC": "true",
                    "AUTHENTICATION_APIKEY_ENABLED": "true",
                    "AUTHENTICATION_APIKEY_ALLOWED_KEYS": "viewer-key,editor-key,admin-key,custom-key",
                    "AUTHENTICATION_APIKEY_USERS": "viewer-user,editor-user,admin-user,custom-user",
                    "AUTHENTICATION_APIKEY_ROLES": "viewer,editor,admin,custom",
                }
            )

        if self.cluster_type == ClusterType.SINGLE_NODE:
            self.nodes.append(
                NodeConfig(
                    http_port=base_port,
                    grpc_port=base_grpc_port,
                    hostname=f"weaviate-{self.instance_type.value}",
                    environment={
                        **base_env,
                        "CLUSTER_HOSTNAME": f"weaviate-{self.instance_type.value}",
                        "RAFT_BOOTSTRAP_EXPECT": "1",
                    },
                )
            )
        else:
            for i in range(3):
                self.nodes.append(
                    NodeConfig(
                        http_port=base_port + i,
                        grpc_port=base_grpc_port + i,
                        hostname=f"weaviate-{self.instance_type.value}-{i+1}",
                        environment={
                            **base_env,
                            "CLUSTER_HOSTNAME": f"weaviate-{self.instance_type.value}-{i+1}",
                            "CLUSTER_GOSSIP_BIND_PORT": f"{7100 + (i*2)}",
                            "CLUSTER_DATA_BIND_PORT": f"{7101 + (i*2)}",
                            "RAFT_BOOTSTRAP_EXPECT": "1",
                            "RAFT_JOIN": f"weaviate-{self.instance_type.value}-1",
                            "CLUSTER_JOIN": f"weaviate-{self.instance_type.value}-1:7100",
                        },
                    )
                )

    def write_compose_file(self) -> str:
        compose = {
            "version": "3.4",
            "services": (
                {
                    "contextionary": {
                        "container_name": "contextionary",
                        "image": "semitechnologies/contextionary:en0.16.0-v1.2.1",
                        "ports": ["9999:9999"],
                        "environment": {
                            "OCCURRENCE_WEIGHT_LINEAR_FACTOR": "0.75",
                            "EXTENSIONS_STORAGE_MODE": "weaviate",
                            "EXTENSIONS_STORAGE_ORIGIN": "http://weaviate:8080",
                        },
                    }
                }
                if self.instance_type == InstanceType.REGULAR
                else {}
            ),
        }

        for index, node in enumerate(self.nodes):
            compose["services"][node.hostname] = {
                "container_name": node.hostname,
                "image": "weaviate/test-server",
                "build": {"context": ".", "dockerfile": "Dockerfile", "target": "weaviate"},
                "restart": "on-failure:0",
                "ports": [
                    f"{node.http_port}:8080",
                    f"{node.grpc_port}:50051",
                    f"{6060 + index}:6060",
                    f"{2112 + index}:2112",
                ],
                "environment": {
                    **node.environment,
                },
            }

        fd, path = tempfile.mkstemp(prefix="docker-compose-", suffix=".yml")
        with open(fd, "w") as f:
            yaml.dump(compose, f)

        return path
