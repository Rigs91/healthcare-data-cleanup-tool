param(
  [string]$BaseUrl = "http://127.0.0.1:8000",
  [string]$SampleFile = "",
  [ValidateSet("deterministic", "ollama_assisted")]
  [string]$CleanupMode = "deterministic",
  [string]$LlmModel = ""
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\common.ps1"
Add-Type -AssemblyName System.Net.Http

function New-HttpClient {
  $handler = New-Object System.Net.Http.HttpClientHandler
  $client = New-Object System.Net.Http.HttpClient($handler)
  $client.Timeout = [TimeSpan]::FromSeconds(60)
  return $client
}

function Read-JsonBody {
  param([System.Net.Http.HttpResponseMessage]$Response)
  $text = $Response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
  if (-not $text) {
    return $null
  }
  try {
    return $text | ConvertFrom-Json
  } catch {
    return $text
  }
}

function Assert-SuccessResponse {
  param(
    [System.Net.Http.HttpResponseMessage]$Response,
    [string]$Operation
  )

  if ($Response.IsSuccessStatusCode) {
    return
  }

  $payload = Read-JsonBody -Response $Response
  $message = if ($payload -is [string]) {
    $payload
  } elseif ($payload.detail) {
    if ($payload.detail.message) { $payload.detail.message } else { $payload.detail | ConvertTo-Json -Depth 20 -Compress }
  } else {
    $payload | ConvertTo-Json -Depth 20 -Compress
  }

  if (-not $message) {
    $message = $Response.ReasonPhrase
  }

  throw "$Operation failed with HTTP $([int]$Response.StatusCode): $message"
}

function Invoke-JsonPost {
  param(
    [System.Net.Http.HttpClient]$Client,
    [string]$Uri,
    [object]$Body
  )

  $json = $Body | ConvertTo-Json -Depth 50
  $content = New-Object System.Net.Http.StringContent($json, [System.Text.Encoding]::UTF8, "application/json")
  $response = $Client.PostAsync($Uri, $content).GetAwaiter().GetResult()
  Assert-SuccessResponse -Response $response -Operation "POST $Uri"
  return Read-JsonBody -Response $response
}

function Invoke-JsonGet {
  param(
    [System.Net.Http.HttpClient]$Client,
    [string]$Uri
  )

  $response = $Client.GetAsync($Uri).GetAwaiter().GetResult()
  Assert-SuccessResponse -Response $response -Operation "GET $Uri"
  return Read-JsonBody -Response $response
}

function Invoke-MultipartUpload {
  param(
    [System.Net.Http.HttpClient]$Client,
    [string]$Uri,
    [string]$FilePath,
    [hashtable]$Fields
  )

  $content = New-Object System.Net.Http.MultipartFormDataContent
  foreach ($entry in $Fields.GetEnumerator()) {
    $stringContent = New-Object System.Net.Http.StringContent([string]$entry.Value, [System.Text.Encoding]::UTF8)
    $content.Add($stringContent, $entry.Key)
  }

  $stream = [System.IO.File]::OpenRead($FilePath)
  try {
    $fileContent = New-Object System.Net.Http.StreamContent($stream)
    $fileContent.Headers.ContentType = [System.Net.Http.Headers.MediaTypeHeaderValue]::Parse("text/csv")
    $content.Add($fileContent, "file", [System.IO.Path]::GetFileName($FilePath))

    $response = $Client.PostAsync($Uri, $content).GetAwaiter().GetResult()
    Assert-SuccessResponse -Response $response -Operation "POST $Uri"
    return Read-JsonBody -Response $response
  } finally {
    $stream.Dispose()
  }
}

function Download-File {
  param(
    [System.Net.Http.HttpClient]$Client,
    [string]$Uri,
    [string]$DestinationPath
  )

  $response = $Client.GetAsync($Uri).GetAwaiter().GetResult()
  Assert-SuccessResponse -Response $response -Operation "GET $Uri"
  $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
  [System.IO.File]::WriteAllBytes($DestinationPath, $bytes)
}

$repoRoot = Get-RepoRoot -StartPath $PSScriptRoot
if (-not $SampleFile) {
  $SampleFile = Join-Path $repoRoot "data\sample_messy.csv"
}
if (-not (Test-Path -LiteralPath $SampleFile)) {
  throw "Sample file not found: $SampleFile"
}

$normalizedBaseUrl = $BaseUrl.TrimEnd("/")
$targetHealth = Invoke-ApiGetJson -Uri "$normalizedBaseUrl/api/health" -TimeoutSeconds 2
$targetMatches = $targetHealth `
  -and $targetHealth.PSObject.Properties.Name -contains "service" `
  -and $targetHealth.service -eq "hc-data-cleanup-ai"

if (-not $targetMatches) {
  $launcherState = Read-LauncherState -RepoRoot $repoRoot
  $launcherBaseUrl = [string](Get-ObjectPropertyValue -Object $launcherState -Name "base_url" -Default "")
  $launcherBaseUrl = $launcherBaseUrl.TrimEnd("/")

  if ($launcherBaseUrl -and $launcherBaseUrl -ne $normalizedBaseUrl) {
    $launcherHealth = Invoke-ApiGetJson -Uri "$launcherBaseUrl/api/health" -TimeoutSeconds 2
    $launcherMatches = $launcherHealth `
      -and $launcherHealth.PSObject.Properties.Name -contains "service" `
      -and $launcherHealth.service -eq "hc-data-cleanup-ai"

    if ($launcherMatches) {
      $normalizedBaseUrl = $launcherBaseUrl
      Write-Host "Requested API base URL was unavailable. Using last launcher URL: $normalizedBaseUrl"
    }
  }
}

$client = New-HttpClient

try {
  Write-Host "Using API base URL: $normalizedBaseUrl"
  Write-Host "1) Health check"
  $healthResponse = $client.GetAsync("$normalizedBaseUrl/api/health").GetAwaiter().GetResult()
  Assert-SuccessResponse -Response $healthResponse -Operation "GET /api/health"
  $health = Read-JsonBody -Response $healthResponse
  Write-Host "Health: service=$($health.service) version=$($health.version) workflow=$($health.ui_workflow_version)"

  if ($CleanupMode -eq "ollama_assisted") {
    $providerUri = "$normalizedBaseUrl/api/providers/ollama/models"
    if ($LlmModel) {
      $providerUri = "${providerUri}?requested_model=$([uri]::EscapeDataString($LlmModel))"
    }
    $provider = Invoke-JsonGet -Client $client -Uri $providerUri
    if ($provider.installed_models) {
      Write-Host "Installed local models: $(@($provider.installed_models) -join ', ')"
    }
    if ($provider.models) {
      Write-Host "Planner-safe selectable models: $(@($provider.models) -join ', ')"
    }
    if ($provider.filtered_models) {
      $filtered = @($provider.filtered_models)
      if ($filtered.Count -gt 0) {
        Write-Host "Filtered local models:"
        foreach ($item in $filtered) {
          $name = $item.name
          $reason = $item.reason
          Write-Host "- $name ($reason)"
        }
      }
    }
    if ($LlmModel -and ($provider.models -notcontains $LlmModel)) {
      $reason = if ($provider.filtered_models) {
        ($provider.filtered_models | Where-Object { $_.name -eq $LlmModel } | Select-Object -First 1).reason
      } else {
        ""
      }
      $detail = if ($reason) { "$LlmModel ($reason)" } else { $LlmModel }
      throw "Requested Ollama model '$detail' is not selectable for planner-safe cleanup."
    }
    if (-not $LlmModel -and ($provider.models)) {
      $LlmModel = [string]$provider.models[0]
      Write-Host "Defaulting to planner-safe model: $LlmModel"
    }
  }

  Write-Host "2) Upload workflow"
  $uploadFields = @{
    name = "Sample messy dataset"
    usage_intent = "training"
    cleanup_mode = $CleanupMode
  }
  if ($LlmModel) {
    $uploadFields["llm_model"] = $LlmModel
  }
  $workflow = Invoke-MultipartUpload -Client $client -Uri "$normalizedBaseUrl/api/v2/workflows/upload" -FilePath $SampleFile -Fields $uploadFields
  $workflowId = [string]$workflow.workflow_id
  if (-not $workflowId) {
    throw "Upload did not return a workflow_id."
  }
  Write-Host "Workflow ID: $workflowId"

  Write-Host "3) Run autopilot"
  $autopilotPayload = @{
    target_score = 95
    output_format = "csv"
    privacy_mode = "safe_harbor"
    performance_mode = "balanced"
    cleanup_mode = $CleanupMode
  }
  if ($LlmModel) {
    $autopilotPayload["llm_model"] = $LlmModel
  }
  $runResult = Invoke-JsonPost -Client $client -Uri "$normalizedBaseUrl/api/v2/workflows/$workflowId/autopilot-run" -Body $autopilotPayload
  $execution = $runResult.execution
  Write-Host "Execution mode: $($execution.cleanup_mode)"
  if ($execution.llm_model) {
    Write-Host "LLM model: $($execution.llm_model)"
  }
  if ($execution.llm_plan_status) {
    Write-Host "LLM plan status: $($execution.llm_plan_status)"
  }

  Write-Host "4) Download cleaned dataset"
  $downloadPath = Join-Path $repoRoot "data\cleaned\demo_cleaned.csv"
  Download-File -Client $client -Uri "$normalizedBaseUrl/api/v2/workflows/$workflowId/export" -DestinationPath $downloadPath
  Write-Host "Saved cleaned file to: $downloadPath"

  Write-Host "PASS: scripted workflow completed"
  exit 0
} catch {
  Write-Error $_
  exit 1
} finally {
  $client.Dispose()
}
