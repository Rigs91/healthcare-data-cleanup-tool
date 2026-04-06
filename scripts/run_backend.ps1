param(
  [int]$Port = 8000,
  [string]$BindHost = "127.0.0.1",
  [ValidateSet("v2_legacy", "v3_guided")]
  [string]$WorkflowVersion = "v3_guided",
  [switch]$Reload
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\common.ps1"

$repoRoot = Get-RepoRoot -StartPath $PSScriptRoot
$pythonExe = Ensure-BackendDependencies -RepoRoot $repoRoot
Initialize-BackendEnvironment -RepoRoot $repoRoot -WorkflowVersion $WorkflowVersion

$arguments = @("-m", "uvicorn", "app.main:app", "--host", $BindHost, "--port", "$Port")
if ($Reload) {
  $arguments += "--reload"
}

Push-Location $repoRoot
try {
  & $pythonExe @arguments
  exit $LASTEXITCODE
} finally {
  Pop-Location
}
