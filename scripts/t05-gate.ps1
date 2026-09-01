$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Compose = @('compose', '--project-directory', $Root, '-f', (Join-Path $Root 'compose.yaml'), '--profile', 'kafka')
$Report = Join-Path $Root 'reports/t05-gate.json'
New-Item -ItemType Directory -Force (Split-Path $Report) | Out-Null

function Invoke-Compose {
  param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
  & docker @Compose @Arguments
  if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($Arguments -join ' ')" }
}
function Invoke-ComposeCapture {
  param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
  $output = & docker @Compose @Arguments 2>&1
  if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($Arguments -join ' ')`n$output" }
  return ($output -join "`n")
}
function Invoke-Kafka {
  param([string]$Command)
  Invoke-Compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec $Command
}
function Invoke-KafkaCapture {
  param([string]$Command)
  Invoke-ComposeCapture run --rm --no-deps --entrypoint /bin/bash kafka-init -ec $Command
}
function Assert-ComposeClean {
  $remaining = Invoke-ComposeCapture ps -q
  if ($remaining.Trim()) { throw "Compose resources remain: $remaining" }
  & docker network inspect kafkanuts-kafka *> $null
  if ($LASTEXITCODE -eq 0) { throw 'kafkanuts-kafka network remains after cleanup' }
}

$clean = $false
try {
  Invoke-Compose up -d kafka schema-registry ksqldb
  $ready = $false
  for ($i = 0; $i -lt 60; $i++) {
    try { Invoke-Kafka 'curl -fsS http://ksqldb:8088/info >/dev/null'; $ready = $true; break } catch { Start-Sleep -Seconds 2 }
  }
  if (-not $ready) { throw 'ksqlDB did not become ready' }
  Invoke-Compose run --rm kafka-init

  Invoke-Kafka "printf 'm0-event-1\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic orders"
  $m0 = Invoke-KafkaCapture "kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 1 --timeout-ms 10000"
  if ($m0.Trim() -ne 'm0-event-1') { throw 'M0 consume failed' }

  Invoke-Kafka "curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{\"compatibility\":\"BACKWARD_TRANSITIVE\"}' http://schema-registry:8081/config/orders-value"
  $v1 = '{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"EventEnvelope\",\"namespace\":\"com.bitrockteam.kafkanuts.contracts\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"eventVersion\",\"type\":\"int\"}]}"}'
  $v2 = '{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"EventEnvelope\",\"namespace\":\"com.bitrockteam.kafkanuts.contracts\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"eventVersion\",\"type\":\"int\"},{\"name\":\"tenant\",\"type\":\"string\",\"default\":\"default\"}]}"}'
  $v3 = '{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"EventEnvelope\",\"namespace\":\"com.bitrockteam.kafkanuts.contracts\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"eventVersion\",\"type\":\"string\"}]}"}'
  Invoke-Kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '$v1' http://schema-registry:8081/subjects/orders-value/versions"
  Invoke-Kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '$v2' http://schema-registry:8081/subjects/orders-value/versions"
  $incompatible = Invoke-KafkaCapture "curl -sS -o /tmp/incompatible.json -w '%{http_code}' -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '$v3' http://schema-registry:8081/subjects/orders-value/versions"
  if ($incompatible.Trim() -ne '409') { throw "incompatible schema was not rejected: $incompatible" }
  $compat = Invoke-KafkaCapture 'curl -fsS http://schema-registry:8081/config/orders-value'
  if ($compat -notmatch 'BACKWARD_TRANSITIVE') { throw "wrong compatibility: $compat" }

  $payment = '{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"PaymentEvent\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"payment_status\",\"type\":\"string\"}]}"}'
  Invoke-Kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '$payment' http://schema-registry:8081/subjects/payments-value/versions"
  $createStream = '{"ksql":"CREATE STREAM payment_events (event_id VARCHAR KEY, payment_status VARCHAR) WITH (KAFKA_TOPIC=''payments'', VALUE_FORMAT=''AVRO'');","streamsProperties":{}}'
  $createTable = '{"ksql":"CREATE TABLE payment_status_summary AS SELECT payment_status, COUNT(*) AS payment_count FROM payment_events GROUP BY payment_status EMIT CHANGES;","streamsProperties":{}}'
  Invoke-Kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data '$createStream' http://ksqldb:8088/ksql"
  Invoke-Kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data '$createTable' http://ksqldb:8088/ksql"
  if ((Invoke-KafkaCapture "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data '{\"ksql\":\"DESCRIBE payment_events;\",\"streamsProperties\":{}}' http://ksqldb:8088/ksql") -notmatch 'PAYMENT_EVENTS') { throw 'ksql stream cannot be described' }
  if ((Invoke-KafkaCapture "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data '{\"ksql\":\"DESCRIBE payment_status_summary;\",\"streamsProperties\":{}}' http://ksqldb:8088/ksql") -notmatch 'PAYMENT_STATUS_SUMMARY') { throw 'ksql table cannot be described' }

  Invoke-Compose restart kafka
  $brokerReady = $false
  for ($i = 0; $i -lt 60; $i++) {
    try { Invoke-Kafka 'kafka-broker-api-versions --bootstrap-server kafka:9092 >/dev/null'; $brokerReady = $true; break } catch { Start-Sleep -Seconds 2 }
  }
  if (-not $brokerReady) { throw 'broker did not become ready after restart' }
  Invoke-Kafka "printf 'restart-event-1\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic orders"
  $afterRestart = Invoke-KafkaCapture "kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 2 --timeout-ms 10000"
  if ($afterRestart -notmatch 'm0-event-1' -or $afterRestart -notmatch 'restart-event-1') { throw 'retention/restart verification failed' }

  Invoke-Compose down --volumes --remove-orphans
  Assert-ComposeClean
  $clean = $true
  [ordered]@{task='T05';result='PASS';m0=@{status='PASS';evidence='orders produced and consumed'};schema_compatibility=@{status='PASS';mode='BACKWARD_TRANSITIVE';evidence='canonical v1/v2 accepted; incompatible v3 returned HTTP 409'};ksqldb=@{status='PASS';evidence='real statements accepted and stream/table described'};restart=@{status='PASS';evidence='broker ready after restart; retained m0 and accepted new event'};cleanup=@{status='PASS';evidence='compose ps empty and network absent after verified down'}} | ConvertTo-Json -Depth 6 | Set-Content -Encoding utf8 $Report
  Get-Content $Report
} finally {
  if (-not $clean) {
    try { Invoke-Compose down --volumes --remove-orphans } catch { Write-Error $_ }
  }
}
