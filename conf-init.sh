curl "http://127.0.0.1:9080/apisix/admin/upstreams/1" -H 'X-API-KEY: edd1c9f034335f136f87ad84b625c8f1' -X PUT -d '
{
    "type": "roundrobin",
    "nodes": {
        "127.0.0.1:9080/mock/": 1
    }
}' | jq


curl "http://127.0.0.1:9080/apisix/admin/routes/1" -H 'X-API-KEY: edd1c9f034335f136f87ad84b625c8f1' -X PUT -d '
{
    "uri": "/test",
    "plugins": {
        "axzarbac": {
          "body": "test-from-axza"
        }
    },
    "upstream_id": 1
}' | jq