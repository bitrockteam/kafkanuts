$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Compose = @('compose', '--project-directory', $Root, '-f', (Join-Path $Root 'compose.yaml'), '--profile', 'kafka')
$Report = Join-Path $Root 'reports/t05-gate.json'
$KafkaNetworkName = if ($env:T05_KAFKA_NETWORK_NAME) { $env:T05_KAFKA_NETWORK_NAME } else { 'kafkanuts-kafka' }
New-Item -ItemType Directory -Force (Split-Path $Report) | Out-Null

function Invoke-Compose {
  param([string[]]$Arguments)
  $savedErrorActionPreference = $ErrorActionPreference
  try {
    $ErrorActionPreference = 'Continue'
    & docker @Compose @Arguments
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $savedErrorActionPreference
  }
  if ($exitCode -ne 0) { throw "docker compose failed: $($Arguments -join ' ') (exit $exitCode)" }
}
function Invoke-ComposeCapture {
  param([string[]]$Arguments)
  $savedErrorActionPreference = $ErrorActionPreference
  try {
    $ErrorActionPreference = 'Continue'
    $output = & docker @Compose @Arguments
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $savedErrorActionPreference
  }
  if ($exitCode -ne 0) { throw "docker compose failed: $($Arguments -join ' ') (exit $exitCode)`n$output" }
  return ($output -join "`n")
}
function New-KafkaScriptFile {
  param([string]$ScriptText)
  $name = "t05-kafka-$([guid]::NewGuid().ToString('N')).sh"
  $hostPath = Join-Path (Join-Path $Root 'reports') $name
  $containerPath = "/workspace/reports/$name"
  $normalized = $ScriptText -replace "`r`n", "`n" -replace "`r", "`n"
  $content = "set -e`nset -o pipefail`n$normalized`n"
  $utf8NoBom = New-Object System.Text.UTF8Encoding -ArgumentList $false
  [IO.File]::WriteAllText($hostPath, $content, $utf8NoBom)
  [pscustomobject]@{ HostPath = $hostPath; ContainerPath = $containerPath }
}
function Invoke-Kafka {
  param([string]$ScriptText)
  $savedErrorActionPreference = $ErrorActionPreference
  $temporary = New-KafkaScriptFile $ScriptText
  try {
    $ErrorActionPreference = 'Continue'
    & docker @Compose @('run', '-T', '--rm', '--no-deps', '--entrypoint', '/bin/bash', 'kafka-init', $temporary.ContainerPath)
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $savedErrorActionPreference
    Remove-Item -LiteralPath $temporary.HostPath -Force -ErrorAction SilentlyContinue
  }
  if ($exitCode -ne 0) { throw "Kafka script failed (exit $exitCode)`n$ScriptText" }
}
function Invoke-KafkaCapture {
  param([string]$ScriptText)
  $savedErrorActionPreference = $ErrorActionPreference
  $temporary = New-KafkaScriptFile $ScriptText
  try {
    $ErrorActionPreference = 'Continue'
    $output = & docker @Compose @('run', '-T', '--rm', '--no-deps', '--entrypoint', '/bin/bash', 'kafka-init', $temporary.ContainerPath)
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $savedErrorActionPreference
    Remove-Item -LiteralPath $temporary.HostPath -Force -ErrorAction SilentlyContinue
  }
  if ($exitCode -ne 0) { throw "Kafka script failed (exit $exitCode)`n$ScriptText`n$output" }
  return ($output -join "`n")
}
function Assert-ComposeClean {
  $remaining = Invoke-ComposeCapture @('ps', '-q')
  if ($remaining.Trim()) { throw "Compose resources remain: $remaining" }
  & docker network inspect $KafkaNetworkName *> $null
  if ($LASTEXITCODE -eq 0) { throw "$KafkaNetworkName network remains after cleanup" }
}

$clean = $false
try {
  Invoke-Compose @('up', '-d', 'kafka', 'schema-registry', 'ksqldb')
  $ready = $false
  for ($i = 0; $i -lt 60; $i++) {
    try { Invoke-Kafka 'curl -fsS http://ksqldb:8088/info >/dev/null'; $ready = $true; break } catch { Start-Sleep -Seconds 2 }
  }
  if (-not $ready) { throw 'ksqlDB did not become ready' }
  Invoke-Compose @('run', '--rm', 'kafka-init')

  Invoke-Kafka "printf 'm0-event-1\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic orders"
  $m0 = Invoke-KafkaCapture "kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 1 --timeout-ms 10000"
  if ($m0.Trim() -ne 'm0-event-1') { throw 'M0 consume failed' }

  Invoke-Kafka "curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{\"compatibility\":\"BACKWARD_TRANSITIVE\"}' http://schema-registry:8081/config/orders-value"
  Invoke-Kafka 'schema=$(python3 -c "import json,sys; print(json.dumps(dict(schemaType=\"AVRO\",schema=json.dumps(json.load(open(sys.argv[1])),separators=(\",\",\":\")))))" /workspace/modules/event-contracts/src/main/avro/EventEnvelope.avsc); curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$schema" http://schema-registry:8081/subjects/orders-value/versions'
  Invoke-Kafka 'schema=$(python3 -c "import json,sys; print(json.dumps(dict(schemaType=\"AVRO\",schema=json.dumps(json.load(open(sys.argv[1])),separators=(\",\",\":\")))))" /workspace/scripts/fixtures/EventEnvelope-compatible.avsc); curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$schema" http://schema-registry:8081/subjects/orders-value/versions'
  $incompatible = Invoke-KafkaCapture 'schema=$(python3 -c "import json,sys; print(json.dumps(dict(schemaType=\"AVRO\",schema=json.dumps(json.load(open(sys.argv[1])),separators=(\",\",\":\")))))" /workspace/scripts/fixtures/EventEnvelope-incompatible.avsc); code=$(curl -sS -o /tmp/incompatible.json -w "%{http_code}" -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "$schema" http://schema-registry:8081/subjects/orders-value/versions); test "$code" = 409'
  if ($incompatible.Trim() -ne '409') { throw "incompatible schema was not rejected: $incompatible" }
  $compat = Invoke-KafkaCapture 'curl -fsS http://schema-registry:8081/config/orders-value'
  if ($compat -notmatch 'BACKWARD_TRANSITIVE') { throw "wrong compatibility: $compat" }

  $payment = '{"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"PaymentEvent\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"payment_status\",\"type\":\"string\"}]}"}'
  Invoke-Kafka "curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '$payment' http://schema-registry:8081/subjects/payments-value/versions"
  Invoke-Kafka 'python3 /workspace/scripts/ksql-apply.py /workspace/modules/kafka-baseline/src/main/resources/ksqldb/payment-summary.sql'
  $describeStream = '{"ksql":"DESCRIBE payment_events;","streamsProperties":{}}'
  $describeTable = '{"ksql":"DESCRIBE payment_status_summary;","streamsProperties":{}}'
  if ((Invoke-KafkaCapture "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data '$describeStream' http://ksqldb:8088/ksql") -notmatch 'PAYMENT_EVENTS') { throw 'ksql stream cannot be described' }
  if ((Invoke-KafkaCapture "curl -fsS -X POST -H 'Content-Type: application/vnd.ksql.v1+json' --data '$describeTable' http://ksqldb:8088/ksql") -notmatch 'PAYMENT_STATUS_SUMMARY') { throw 'ksql table cannot be described' }

  Invoke-Compose @('restart', 'kafka')
  $brokerReady = $false
  for ($i = 0; $i -lt 60; $i++) {
    try { Invoke-Kafka 'kafka-broker-api-versions --bootstrap-server kafka:9092 >/dev/null'; $brokerReady = $true; break } catch { Start-Sleep -Seconds 2 }
  }
  if (-not $brokerReady) { throw 'broker did not become ready after restart' }
  Invoke-Kafka "printf 'restart-event-1\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic orders"
  $afterRestart = Invoke-KafkaCapture "kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 2 --timeout-ms 10000"
  if ($afterRestart -notmatch 'm0-event-1' -or $afterRestart -notmatch 'restart-event-1') { throw 'retention/restart verification failed' }

  Invoke-Compose @('down', '--volumes', '--remove-orphans')
  Assert-ComposeClean
  $clean = $true
  [ordered]@{task='T05';result='PASS';m0=@{status='PASS';evidence='orders produced and consumed'};schema_compatibility=@{status='PASS';mode='BACKWARD_TRANSITIVE';artifact='modules/event-contracts/src/main/avro/EventEnvelope.avsc plus separate fixture files';evidence='canonical artifact v1 and compatible/incompatible fixture artifacts exercised; incompatible returned HTTP 409'};ksqldb=@{status='PASS';artifact='modules/kafka-baseline/src/main/resources/ksqldb/payment-summary.sql';evidence='versioned production SQL applied and stream/table described'};restart=@{status='PASS';evidence='broker ready after restart; retained m0 and accepted new event'};cleanup=@{status='PASS';network=$KafkaNetworkName;evidence='compose ps empty and effective network absent after verified down'}} | ConvertTo-Json -Depth 6 | Set-Content -Encoding utf8 $Report
  Get-Content $Report
} finally {
  if (-not $clean) {
    try { Invoke-Compose @('down', '--volumes', '--remove-orphans') } catch { Write-Error $_ }
  }
}
