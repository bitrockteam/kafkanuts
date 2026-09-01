[CmdletBinding()]
param([ValidateSet('doctor','config','up','down','status','reset')][string]$Command = 'doctor')

$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ComposeArgs = @('--project-directory', $Root, '-f', (Join-Path $Root 'compose.yaml'))

function Invoke-Compose([string[]]$Arguments) {
  & docker compose @ComposeArgs @Arguments
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

switch ($Command) {
  'doctor' {
    & docker version
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Invoke-Compose @('config')
    Write-Output 'doctor: OK'
  }
  'config' { Invoke-Compose @('config') }
  'up' { Invoke-Compose @('--profile','bootstrap','up','--build','--abort-on-container-exit') }
  'down' { Invoke-Compose @('down') }
  'status' { Invoke-Compose @('ps','--all') }
  'reset' { Invoke-Compose @('down','--volumes','--remove-orphans') }
}
