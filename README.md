# Architecture

```mermaid
flowchart
subgraph Primary
    Proxy1[Proxy 1]--Forward request-->Redis1
    end

subgraph Replica
    Proxy2[Proxy 2]--Forward request-->Redis2
    end


Client1 --Write request--> Proxy1
Client2 --Read request--> Proxy2
Proxy1--Publish message---->DistributedLog
Proxy2--Consume message---->DistributedLog
Proxy1<--Cluster coordination---->etcd
Proxy2<--Cluster coordination---->etcd
```

