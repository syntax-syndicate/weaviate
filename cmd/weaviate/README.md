## What is this?

tldr; use `cmd/weaviate-server` instead. This is experimental

## How to run locally

```sh
# reset env
./tools/dev/restart_dev_environment.sh

#shell1 (minio)
docker run \
--rm \
-p 9000:9000 \
-p 9001:9001 \
--user $(id -u):$(id -g) \
--name minio1 \
-e "MINIO_ROOT_USER=aws_access_key" \
-e "MINIO_ROOT_PASSWORD=aws_secret_key" \
-v ${HOME}/minio/data:/data \
quay.io/minio/minio server /data --console-address ":9001"

#shell2 (weaviate0)
clear && OFFLOAD_S3_ENDPOINT="http://localhost:9000" OFFLOAD_S3_BUCKET_AUTO_CREATE="true" ./tools/dev/run_dev_server.sh local-offload-s3

#shell3 (weaviate1)
OFFLOAD_S3_ENDPOINT="http://localhost:9000" OFFLOAD_S3_BUCKET_AUTO_CREATE="true" ./tools/dev/run_dev_server.sh second-offload-s3

#shell4 (weaviate2)
OFFLOAD_S3_ENDPOINT="http://localhost:9000" OFFLOAD_S3_BUCKET_AUTO_CREATE="true" ./tools/dev/run_dev_server.sh third-offload-s3

#shell5 (querier)
go build ./cmd/weaviate && ./weaviate --target=querier
```

Create a class/tenant and insert some data, for example using this python script (need to pip install `weaviate-client`):

```python
import time
import weaviate
import weaviate.classes.config as wvcc
import weaviate.collections.classes.tenants as t

client = weaviate.connect_to_local()

client.collections.delete_all()

collection_name = "Question"
tenant_name = "kavi"
collection = client.collections.create(
    name=collection_name,
    properties=[
        wvcc.Property(
            name="title",
            data_type=wvcc.DataType.TEXT
        )
    ],
    multi_tenancy_config=wvcc.Configure.multi_tenancy(enabled=True),

)
tenant = t.Tenant(name=tenant_name, activity_status=t.TenantCreateActivityStatus.ACTIVE)
collection.tenants.create(tenant)
collection_tenant = collection.with_tenant(tenant)
uuid = collection_tenant.data.insert(
    properties={
        "title": "foo",
    },
    vector=[0.1, 0.2],
)
tenant_offloaded = t.Tenant(name=tenant_name, activity_status=t.TenantUpdateActivityStatus.OFFLOADED)
collection.tenants.update(t1_offloaded)

client.close()
```

Submit a grpc search to the querier:

```sh
grpcurl -plaintext -d '{"collection": "'"Question"'", "tenant": "kavi", "near_text": { "query": ["biology"], "certainty": 0.8}, "limit": 2}' localhost:9090 weaviate.v1.Weaviate.Search
```
