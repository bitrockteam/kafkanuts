$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Compose = @('compose', '--project-directory', $Root, '-f', (Join-Path $Root 'compose.yaml'), '--profile', 'kafka', '--profile', 't06')
$Network = if ($env:T06_NETWORK_NAME) { $env:T06_NETWORK_NAME } else { 'kafkanuts-flink-kafka' }
$Report = Join-Path $Root 'reports/t06-gate.json'
New-Item -ItemType Directory -Force (Split-Path $Report) | Out-Null
function Invoke-Compose {
  param([string[]]$Arguments)
  $saved = $ErrorActionPreference
  try { $ErrorActionPreference = 'Continue'; & docker @Compose @Arguments; $exitCode = $LASTEXITCODE } finally { $ErrorActionPreference = $saved }
  if ($exitCode -ne 0) { throw "docker compose failed (exit $exitCode): $($Arguments -join ' ')" }
}
function Invoke-ComposeCapture {
  param([string[]]$Arguments)
  $saved = $ErrorActionPreference
  try { $ErrorActionPreference = 'Continue'; $output = & docker @Compose @Arguments; $exitCode = $LASTEXITCODE } finally { $ErrorActionPreference = $saved }
  if ($exitCode -ne 0) { throw "docker compose failed (exit $exitCode): $($Arguments -join ' ')`n$output" }
  return ($output -join "`n")
}
function Assert-Clean {
  if ((Invoke-ComposeCapture @('ps', '-q')).Trim()) { throw 'T06 Compose resources remain' }
  & docker network inspect $Network *> $null
  if ($LASTEXITCODE -eq 0) { throw "$Network remains after cleanup" }
}
$clean = $false
try {
  Invoke-Compose @('build', 'flink-kafka-submit')
  Invoke-Compose @('up', '-d', 'kafka', 'flink-kafka-jobmanager', 'flink-kafka-taskmanager')
  Invoke-Compose @('run', '--rm', 'kafka-init')
  Invoke-Compose @('run', '--rm', 'flink-kafka-submit')
  Invoke-Compose @('run', '--rm', 't06-gate', 'pre')
  Invoke-Compose @('restart', 'flink-kafka-taskmanager')
  Invoke-Compose @('run', '--rm', 't06-gate', 'recovery')
  Invoke-Compose @('down', '--volumes', '--remove-orphans')
  Assert-Clean
  $clean = $true
  [ordered]@{task='T06';result='PASS';processing=@{status='PASS';evidence='Flink Kafka consumed t06-events and emitted t06-parity-output'};checkpoint=@{status='PASS';evidence='completed checkpoints verified before and after TaskManager restart'};duplicate_recovery=@{status='PASS';evidence='duplicate emitted once before and after restart'};cluster=@{jobmanager='flink-kafka-jobmanager';taskmanager='flink-kafka-taskmanager';network=$Network;checkpoint_volume='flink-kafka-checkpoints'};cleanup=@{status='PASS';evidence='Compose empty and effective network absent'}} | ConvertTo-Json -Depth 6 | Set-Content -Encoding utf8 $Report
  Get-Content $Report
} finally {
  if (-not $clean) { try { Invoke-Compose @('down', '--volumes', '--remove-orphans') } catch { Write-Error $_ } }
}
