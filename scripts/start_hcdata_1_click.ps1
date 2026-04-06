param(
  [int]$PreferredPort = 8000,
  [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\common.ps1"

$repoRoot = Get-RepoRoot -StartPath $PSScriptRoot
$logsDir = Join-Path $repoRoot "logs"
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null

$null = Ensure-BackendDependencies -RepoRoot $repoRoot
$ollamaDirect = Get-OllamaStatus

function Start-LauncherBackend {
  param(
    [int]$Port,
    [string]$RepoRoot,
    [string]$LogsDir
  )

  $stdoutLog = Join-Path $LogsDir "launcher-backend-$Port.out.log"
  $stderrLog = Join-Path $LogsDir "launcher-backend-$Port.err.log"
  $backendScript = Join-Path $RepoRoot "scripts\run_backend.ps1"
  $arguments = "-ExecutionPolicy Bypass -File `"$backendScript`" -Port $Port -BindHost 127.0.0.1 -WorkflowVersion v3_guided"

  Start-Process -FilePath "powershell.exe" `
    -ArgumentList $arguments `
    -WorkingDirectory $RepoRoot `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -WindowStyle Hidden | Out-Null

  return @{
    stdoutLog = $stdoutLog
    stderrLog = $stderrLog
  }
}

$baseUrl = "http://127.0.0.1:$PreferredPort"
$existingHealth = Invoke-ApiGetJson -Uri "$baseUrl/api/health" -TimeoutSeconds 2
$reusedExisting = $false
$replacedStaleExisting = $false
$startedAlternateBecauseStale = $false
$selectedPort = $PreferredPort
$stdoutLog = ""
$stderrLog = ""
$launcherNote = ""
$compatibilityWarning = ""
$existingProviderState = "unusable"

$shouldReuseExisting = $existingHealth `
  -and $existingHealth.PSObject.Properties.Name -contains "service" `
  -and $existingHealth.service -eq "hc-data-cleanup-ai"

if ($shouldReuseExisting) {
  $existingProviderApi = Invoke-ApiGetJson -Uri "$baseUrl/api/providers/ollama/models" -TimeoutSeconds 5
  $existingProviderState = Get-OllamaProviderContractState -Status $existingProviderApi

  if ($existingProviderState -eq "current") {
    $reusedExisting = $true
  } else {
    $stopResult = Stop-HcDataBackendOnPort -Port $PreferredPort -Health $existingHealth -TimeoutSeconds 10
    if ($stopResult.stopped) {
      $launch = Start-LauncherBackend -Port $PreferredPort -RepoRoot $repoRoot -LogsDir $logsDir
      $stdoutLog = $launch.stdoutLog
      $stderrLog = $launch.stderrLog
      $replacedStaleExisting = $true
      $launcherNote = $stopResult.message
    } else {
      $selectedPort = Find-AvailablePort -PreferredPort ($PreferredPort + 1)
      $baseUrl = "http://127.0.0.1:$selectedPort"
      $launch = Start-LauncherBackend -Port $selectedPort -RepoRoot $repoRoot -LogsDir $logsDir
      $stdoutLog = $launch.stdoutLog
      $stderrLog = $launch.stderrLog
      $startedAlternateBecauseStale = $true
      $compatibilityWarning = $stopResult.message
    }
  }
} else {
  $selectedPort = Find-AvailablePort -PreferredPort $PreferredPort
  $baseUrl = "http://127.0.0.1:$selectedPort"
  $launch = Start-LauncherBackend -Port $selectedPort -RepoRoot $repoRoot -LogsDir $logsDir
  $stdoutLog = $launch.stdoutLog
  $stderrLog = $launch.stderrLog
}

$health = if ($reusedExisting) { $existingHealth } else { Wait-ApiHealth -BaseUrl $baseUrl -TimeoutSeconds 60 }
if (-not $health) {
  Write-Host "APP HEALTH: FAIL"
  Write-Host "Frontend did not become ready."
  if ($stdoutLog) {
    Write-Host "Stdout log: $stdoutLog"
  }
  if ($stderrLog) {
    Write-Host "Stderr log: $stderrLog"
  }
  Write-Host "Troubleshooting:"
  Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\dev.ps1 -Port $PreferredPort"
  Write-Host "  Get-Content .\logs\launcher-backend-$selectedPort.err.log -Tail 80"
  exit 1
}

$providerApi = Invoke-ApiGetJson -Uri "$baseUrl/api/providers/ollama/models" -TimeoutSeconds 5
$ollamaStatus = Normalize-OllamaStatus -Status $(if ($providerApi) { $providerApi } else { $ollamaDirect })

Write-LauncherState -RepoRoot $repoRoot -State @{
  base_url = $baseUrl
  port = $selectedPort
  workflow_version = $health.ui_workflow_version
  backend_status = if ($reusedExisting) {
    "reused existing instance"
  } elseif ($replacedStaleExisting) {
    "replaced stale existing instance"
  } elseif ($startedAlternateBecauseStale) {
    "started alternate instance"
  } else {
    "started new instance"
  }
  ollama_reachable = $ollamaStatus.reachable
}

Write-Host "APP HEALTH: PASS"
Write-Host "App URL: $baseUrl"
Write-Host "Workflow default: $($health.ui_workflow_version)"
if ($reusedExisting) {
  Write-Host "Backend process: reused existing instance on port $selectedPort"
} elseif ($replacedStaleExisting) {
  Write-Host "Backend process: replaced stale existing instance on port $selectedPort"
} elseif ($startedAlternateBecauseStale) {
  Write-Host "Backend process: started on alternate port because stale instance could not be replaced (using $selectedPort)"
} else {
  Write-Host "Backend process: started new instance on port $selectedPort"
}
if (-not $reusedExisting) {
  if ($stdoutLog) {
    Write-Host "Stdout log: $stdoutLog"
  }
  if ($stderrLog) {
    Write-Host "Stderr log: $stderrLog"
  }
}
if ($launcherNote) {
  Write-Host "Launcher note: $launcherNote"
}
if ($compatibilityWarning) {
  Write-Host "Launcher warning: $compatibilityWarning"
}
if ($existingProviderState -eq "stale") {
  Write-Host "Launcher note: detected an older Ollama provider contract on the reused backend and replaced it automatically."
} elseif ($existingProviderState -eq "unusable" -and $shouldReuseExisting) {
  Write-Host "Launcher note: detected an incompatible reused backend and replaced it with the current repo version."
}

if ($ollamaStatus.reachable) {
  Write-Host "OLLAMA HEALTH: PASS"
  if ($ollamaStatus.installed_models.Count -gt 0) {
    Write-Host "Installed local models: $($ollamaStatus.installed_models -join ', ')"
  } else {
    Write-Host "Installed local models: none"
  }
  if ($ollamaStatus.models.Count -gt 0) {
    Write-Host "Planner-safe selectable models: $($ollamaStatus.models -join ', ')"
  } else {
    Write-Host "Planner-safe selectable models: none"
  }
  if ($ollamaStatus.filtered_models.Count -gt 0) {
    Write-Host "Filtered local models:"
    foreach ($item in $ollamaStatus.filtered_models) {
      Write-Host (Format-OllamaModelLine -Prefix "-" -Name $item.name -Reason $item.reason)
    }
  }
} else {
  Write-Host "OLLAMA HEALTH: FAIL"
  Write-Host "Ollama was not reachable. Deterministic cleanup remains available."
  if ($ollamaStatus.error) {
    Write-Host "Ollama detail: $($ollamaStatus.error)"
  }
}

Write-Host "Troubleshooting:"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\dev.ps1 -Port $selectedPort"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\run_tests.ps1"
Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\test_tool.ps1 -BaseUrl $baseUrl"
Write-Host "  Get-Content .\logs\launcher-backend-$selectedPort.err.log -Tail 80"
Write-Host "  Older reused backends are now replaced automatically when the Ollama provider contract is stale."
Write-Host "  ollama list"
Write-Host "  ollama serve"

if (-not $NoBrowser) {
  Start-Process $baseUrl | Out-Null
}

exit 0
