from typing import Any, Optional, List, Generator, Protocol, Type, Dict, Tuple, Union, Callable

import pytest
from _pytest.fixtures import SubRequest
from _pytest.config import Config
from _pytest.main import Session
import time
import os
import subprocess
from .cluster_config import WeaviateClusterConfig, ClusterType, InstanceType

import weaviate
from weaviate.collections import Collection
from weaviate.collections.classes.config import (
    Property,
    _VectorizerConfigCreate,
    _InvertedIndexConfigCreate,
    _ReferencePropertyBase,
    _GenerativeProvider,
    _ReplicationConfigCreate,
    _MultiTenancyConfigCreate,
    _VectorIndexConfigCreate,
    _RerankerProvider,
)
from weaviate.collections.classes.types import Properties
from weaviate.config import AdditionalConfig

from weaviate.collections.classes.config_named_vectors import _NamedVectorConfigCreate
import weaviate.classes as wvc


# add option to pytest to specify cluster type
def pytest_addoption(parser):
    parser.addoption(
        "--cluster-type",
        action="store",
        default="single",
        help="cluster type: single or three",
        choices=["single", "three"],
    )
    parser.addoption(
        "--instance-type",
        action="store",
        default="regular",
        help="instance type: regular or rbac",
        choices=["regular", "rbac"],
    )


# Session-level fixture for cluster configuration
@pytest.fixture(scope="session")
def instance_type(request) -> InstanceType:
    return InstanceType(request.config.getoption("--instance-type"))


@pytest.fixture(scope="session")
def cluster_type(request) -> ClusterType:
    return ClusterType(request.config.getoption("--cluster-type"))


def wait_for_ready(port: int, max_retries: int = 30):
    import requests
    from requests.exceptions import RequestException

    for _ in range(max_retries):
        try:
            response = requests.get(f"http://localhost:{port}/v1/.well-known/ready")
            if response.status_code == 200:
                return
        except RequestException:
            pass
        time.sleep(1)
    raise TimeoutError(f"Node on port {port} did not become ready")


# Session-level fixture for Weaviate cluster, autouse=True to start cluster before tests
@pytest.fixture(scope="session", autouse=True)
def weaviate_cluster(cluster_type, instance_type) -> Generator[WeaviateClusterConfig, None, None]:
    compose_file = None

    # TOREMOVE: This makes the parallel tests useless as each worker needs to wait , we will have to create the cluster
    # in a previous step (either using python or bash)
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

    if worker_id == "gw0":
        config = WeaviateClusterConfig(cluster_type, instance_type)
        compose_file = config.write_compose_file()

        # Start cluster
        try:
            result = subprocess.run(
                ["docker", "compose", "-f", compose_file, "up", "-d"],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Docker compose failed with error:\n{e.stderr}")
            raise

        # Wait for all nodes
        for node in config.nodes:
            wait_for_ready(node.http_port)

        yield config

        # Cleanup
        # subprocess.run(["docker", "compose", "-f", compose_file, "down"], check=True)
        # os.unlink(compose_file)
    else:
        # Other workers should wait for the primary worker to set up the cluster
        time.sleep(5)  # Give primary worker time to start cluster
        yield WeaviateClusterConfig(cluster_type, instance_type)

    if compose_file:
        print(
            f"\nTests run finished! To cleanup manually run:\ndocker compose -f {compose_file} down"
        )


class CollectionFactory(Protocol):
    """Typing for fixture."""

    def __call__(
        self,
        name: str = "",
        properties: Optional[List[Property]] = None,
        references: Optional[List[_ReferencePropertyBase]] = None,
        vectorizer_config: Optional[
            Union[_VectorizerConfigCreate, List[_NamedVectorConfigCreate]]
        ] = None,
        inverted_index_config: Optional[_InvertedIndexConfigCreate] = None,
        multi_tenancy_config: Optional[_MultiTenancyConfigCreate] = None,
        generative_config: Optional[_GenerativeProvider] = None,
        headers: Optional[Dict[str, str]] = None,
        ports: Tuple[int, int] = (8080, 50051),
        data_model_properties: Optional[Type[Properties]] = None,
        data_model_refs: Optional[Type[Properties]] = None,
        replication_config: Optional[_ReplicationConfigCreate] = None,
        vector_index_config: Optional[_VectorIndexConfigCreate] = None,
        description: Optional[str] = None,
        reranker_config: Optional[_RerankerProvider] = None,
    ) -> Collection[Any, Any]:
        """Typing for fixture."""
        ...


@pytest.fixture
def weaviate_client() -> Callable[[int, int], weaviate.WeaviateClient]:
    def connect(http_port: int = 8080, grpc_port: int = 50051) -> weaviate.WeaviateClient:
        return weaviate.connect_to_local(
            port=http_port,
            grpc_port=grpc_port,
            additional_config=AdditionalConfig(timeout=(60, 120)),  # for image tests
        )

    return connect


@pytest.fixture
def collection_factory(request: SubRequest) -> Generator[CollectionFactory, None, None]:
    name_fixture: Optional[str] = None
    client_fixture: Optional[weaviate.WeaviateClient] = None

    def _factory(
        name: str = "",
        properties: Optional[List[Property]] = None,
        references: Optional[List[_ReferencePropertyBase]] = None,
        vectorizer_config: Optional[
            Union[_VectorizerConfigCreate, List[_NamedVectorConfigCreate]]
        ] = None,
        inverted_index_config: Optional[_InvertedIndexConfigCreate] = None,
        multi_tenancy_config: Optional[_MultiTenancyConfigCreate] = None,
        generative_config: Optional[_GenerativeProvider] = None,
        headers: Optional[Dict[str, str]] = None,
        ports: Tuple[int, int] = (8080, 50051),
        data_model_properties: Optional[Type[Properties]] = None,
        data_model_refs: Optional[Type[Properties]] = None,
        replication_config: Optional[_ReplicationConfigCreate] = None,
        vector_index_config: Optional[_VectorIndexConfigCreate] = None,
        description: Optional[str] = None,
        reranker_config: Optional[_RerankerProvider] = None,
    ) -> Collection[Any, Any]:
        nonlocal client_fixture, name_fixture
        name_fixture = _sanitize_collection_name(request.node.name) + name
        client_fixture = weaviate.connect_to_local(
            headers=headers,
            grpc_port=ports[1],
            port=ports[0],
            additional_config=AdditionalConfig(timeout=(60, 120)),  # for image tests
        )
        client_fixture.collections.delete(name_fixture)

        collection: Collection[Any, Any] = client_fixture.collections.create(
            name=name_fixture,
            description=description,
            vectorizer_config=vectorizer_config,
            properties=properties,
            references=references,
            inverted_index_config=inverted_index_config,
            multi_tenancy_config=multi_tenancy_config,
            generative_config=generative_config,
            data_model_properties=data_model_properties,
            data_model_references=data_model_refs,
            replication_config=replication_config,
            vector_index_config=vector_index_config,
            reranker_config=reranker_config,
        )
        return collection

    try:
        yield _factory
    finally:
        if client_fixture is not None and name_fixture is not None:
            client_fixture.collections.delete(name_fixture)
            client_fixture.close()


class NamedCollection(Protocol):
    """Typing for fixture."""

    def __call__(self, name: str = "", props: Optional[List[str]] = None) -> Collection:
        """Typing for fixture."""
        ...


@pytest.fixture
def named_collection(
    collection_factory: CollectionFactory,
) -> Generator[NamedCollection, None, None]:
    def _factory(name: str = "", props: Optional[List[str]] = None) -> Collection:
        if props is None:
            props = ["title1", "title2", "title3"]

        properties = [Property(name=prop, data_type=wvc.config.DataType.TEXT) for prop in props]
        named_vectors = [
            wvc.config.Configure.NamedVectors.text2vec_contextionary(
                name=prop.name,
                source_properties=[prop.name],
                vectorize_collection_name=False,
            )
            for prop in properties
        ]

        collection = collection_factory(
            name,
            properties=properties,
            vectorizer_config=named_vectors,
        )

        return collection

    yield _factory


def _sanitize_collection_name(name: str) -> str:
    name = (
        name.replace("[", "")
        .replace("]", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .replace("{", "")
        .replace("}", "")
    )
    return name[0].upper() + name[1:]
