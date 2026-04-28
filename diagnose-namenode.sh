#!/bin/bash

set -e

PROJECT_DIR="$HOME/EC528/hadoop-config-project"
cd "$PROJECT_DIR" || exit 1

echo "=== Docker Compose Status ==="
docker compose ps

echo -e "\n=== Namenode Container Status ==="
docker inspect namenode --format='{{.State.Status}}' || echo "Container not found"

echo -e "\n=== Last 50 Namenode Logs ==="
docker logs namenode --tail 50 2>&1 | tail -20

echo -e "\n=== Safe Mode Check ==="
docker exec -it namenode hdfs dfsadmin -safemode get 2>&1 || echo "dfsadmin command failed"

echo -e "\n=== Port 8020 Connectivity Check ==="
docker exec namenode bash -c 'nc -zv localhost 8020' 2>&1 || echo "Port 8020 not responding"

echo -e "\n=== Namenode Process Check ==="
docker exec namenode ps aux | grep namenode || echo "No namenode process found"

echo -e "\n=== Disk/Storage Check ==="
docker exec namenode df -h /hadoop/dfs/name || echo "Failed to check disk"

echo -e "\nDone with diagnostics."
