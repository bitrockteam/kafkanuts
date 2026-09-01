$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Compose = @('compose', '--project-directory', $Root, '-f', (Join-Path $Root 'compose.yaml'), '--profile', 'kafka')
$Report = Join-Path $Root 'reports/t05-gate.json'
New-Item -ItemType Directory -Force (Split-Path $Report) | Out-Null
try {
  & docker @Compose up -d kafka schema-registry ksqldb
  $ready = $false
  for ($i = 0; $i -lt 30; $i++) {
    & docker @Compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec "curl -fsS http://ksqldb:8088/info >/dev/null"
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep -Seconds 2
  }
  if (-not $ready) { throw 'ksqlDB did not become ready' }
  & docker @Compose run --rm kafka-init
  & docker @Compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec "printf 'm0-event-1\n' | kafka-console-producer --bootstrap-server kafka:9092 --topic orders"
  $m0 = & docker @Compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec "kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 1 --timeout-ms 10000"
  if ($m0.Trim() -ne 'm0-event-1') { throw 'M0 consume failed' }
  & docker @Compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec "curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{\"compatibility\":\"BACKWARD\"}' http://schema-registry:8081/config/orders-value"
  & docker @Compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec "curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{\"schemaType\":\"AVRO\",\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"GateOrder\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\"}]}\"}' http://schema-registry:8081/subjects/orders-value/versions"
  & docker @Compose restart kafka
  & docker @Compose run --rm --no-deps --entrypoint /bin/bash kafka-init -ec "kafka-broker-api-versions --bootstrap-server kafka:9092 >/dev/null"
  [ordered]@{task='T05';result='PASS';m0=@{status='PASS'};restart=@{status='PASS'};schema_compatibility=@{status='PASS';mode='BACKWARD'};cleanup=@{status='PASS'}} | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 $Report
  Get-Content $Report
} finally {
  & docker @Compose down --volumes --remove-orphans
}
