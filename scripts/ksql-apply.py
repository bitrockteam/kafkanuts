#!/usr/bin/env python3
"""Apply each statement from the versioned T05 ksqlDB artifact."""
import json
import sys
import urllib.request

sql_path = sys.argv[1]
sql = "\n".join(
    line for line in open(sql_path, encoding="utf-8") if not line.strip().startswith("--")
)
for statement in sql.split(";"):
    statement = statement.strip()
    if not statement:
        continue
    payload = json.dumps({"ksql": statement + ";", "streamsProperties": {}}).encode()
    request = urllib.request.Request(
        "http://ksqldb:8088/ksql",
        data=payload,
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
    )
    with urllib.request.urlopen(request) as response:
        body = response.read().decode()
        if '"SUCCESS"' not in body:
            raise RuntimeError(body)
        print(body)
