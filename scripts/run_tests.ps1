$ErrorActionPreference = "Stop"
. "$PSScriptRoot\common.ps1"

$repoRoot = Get-RepoRoot -StartPath $PSScriptRoot
$pythonExe = Ensure-BackendDependencies -RepoRoot $repoRoot
Initialize-BackendEnvironment -RepoRoot $repoRoot -WorkflowVersion "v3_guided"

Push-Location $repoRoot
try {
  & $pythonExe -m pytest backend\tests -q
  exit $LASTEXITCODE
} finally {
  Pop-Location
}
