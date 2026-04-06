param(
  [int]$Port = 8000
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\common.ps1"

$repoRoot = Get-RepoRoot -StartPath $PSScriptRoot
$pythonExe = Ensure-BackendDependencies -RepoRoot $repoRoot
Initialize-BackendEnvironment -RepoRoot $repoRoot -WorkflowVersion "v3_guided"

$selectedPort = Find-AvailablePort -PreferredPort $Port
if ($selectedPort -ne $Port) {
  Write-Host "Port $Port is busy. Using $selectedPort instead."
}

$ollama = Get-OllamaStatus
Write-Host "Guided workflow: $env:UI_WORKFLOW_VERSION"
Write-Host "Frontend URL: http://127.0.0.1:$selectedPort"
Write-Host "Ollama reachable: $($ollama.reachable)"
Write-LauncherState -RepoRoot $repoRoot -State @{
  base_url = "http://127.0.0.1:$selectedPort"
  port = $selectedPort
  workflow_version = $env:UI_WORKFLOW_VERSION
  backend_status = "dev"
  ollama_reachable = $ollama.reachable
}
if ($ollama.installed_models.Count -gt 0) {
  Write-Host "Installed local models: $($ollama.installed_models -join ', ')"
} else {
  Write-Host "Installed local models: none"
}
if ($ollama.models.Count -gt 0) {
  Write-Host "Planner-safe selectable models: $($ollama.models -join ', ')"
} else {
  Write-Host "Planner-safe selectable models: none"
}
if ($ollama.filtered_models.Count -gt 0) {
  $filteredLines = @($ollama.filtered_models | ForEach-Object {
    Format-OllamaModelLine -Prefix "-" -Name $_.name -Reason $_.reason
  })
  Write-Host "Filtered local models:"
  foreach ($line in $filteredLines) {
    Write-Host $line
  }
}

Push-Location $repoRoot
try {
  & $pythonExe -m uvicorn app.main:app --reload --host 0.0.0.0 --port $selectedPort
} finally {
  Pop-Location
}
