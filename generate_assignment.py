import json
import sys

topic = 'varadhi_nfr'
replicas = [11,12,13]
partitions = list(range(0,42))

parallel = 2

def rotate(l, n):
    return l[n%len(l):] + l[:n%len(l)]

def to_dict (topic, partition, replicas):
    return {
        "topic": topic, 
        "partition": partition,
        "to": replicas
    }

def generate():
    all = []
    current = []
    i = 0
    for p in partitions:
        current.append(to_dict(topic, p, rotate(replicas, i)))
        i = i + 1
        if i % parallel == 0:
            all.append(current)
            current = []
    return all

def generate_per_replica_sub_plans(plans, i):
    all = []
    for sp in plans[i:]:
        per_replica_sp = []
        for p in sp:
            from_ = p["from"]
            to_ = p["to"]
            per_replica_sp.append({
                "topic": p["topic"],
                "partition": p["partition"],
                "to": [to_[0], from_[1], from_[2]]
            })
        all.append(per_replica_sp)
        per_replica_sp = []

        for p in sp:
            from_ = p["from"]
            to_ = p["to"]
            per_replica_sp.append({
                "topic": p["topic"],
                "partition": p["partition"],
                "to": [to_[0], to_[1], from_[2]]
            })
        all.append(per_replica_sp)
        per_replica_sp = []

        for p in sp:
            from_ = p["from"]
            to_ = p["to"]
            per_replica_sp.append({
                "topic": p["topic"],
                "partition": p["partition"],
                "to": [to_[0], to_[1], to_[2]]
            })
        all.append(per_replica_sp)
        per_replica_sp = []

    print(json.dumps(all, indent=2))

with open(sys.argv[1], "r") as f:
    generate_per_replica_sub_plans(json.load(f), int(sys.argv[2]))
