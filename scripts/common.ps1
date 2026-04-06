Set-StrictMode -Version Latest
$script:OllamaProviderContractVersion = 2

function Get-RepoRoot {
  param([string]$StartPath = $PSScriptRoot)
  return (Resolve-Path (Join-Path $StartPath "..")).Path
}

function Get-LauncherStatePath {
  param([string]$RepoRoot)
  return Join-Path $RepoRoot "logs\launcher-last-run.json"
}

function Get-VenvPath {
  param([string]$RepoRoot)
  return Join-Path $RepoRoot ".venv"
}

function Get-VenvPythonPath {
  param([string]$RepoRoot)
  return Join-Path (Get-VenvPath -RepoRoot $RepoRoot) "Scripts\python.exe"
}

function Get-SystemPythonCommand {
  $python = Get-Command python -ErrorAction SilentlyContinue
  if ($python) {
    return @{
      Command = $python.Source
      Prefix = @()
    }
  }

  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($py) {
    return @{
      Command = $py.Source
      Prefix = @("-3")
    }
  }

  throw "Python was not found on PATH. Install Python 3.11+ and try again."
}

function Get-FileHashHex {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    return ""
  }
  return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash
}

function Get-OllamaModelName {
  param([object]$Model)

  if ($null -eq $Model) {
    return ""
  }

  if ($Model.PSObject.Properties.Name -contains "name") {
    $name = [string]$Model.name
    if ($name.Trim()) {
      return $name.Trim()
    }
  }

  if ($Model.PSObject.Properties.Name -contains "model") {
    $name = [string]$Model.model
    if ($name.Trim()) {
      return $name.Trim()
    }
  }

  return ""
}

function Get-OllamaModelSizeB {
  param([object]$Model)

  if ($null -eq $Model) {
    return $null
  }

  $candidate = ""
  if ($Model.PSObject.Properties.Name -contains "details" -and $Model.details) {
    $details = $Model.details
    if ($details.PSObject.Properties.Name -contains "parameter_size" -and $details.parameter_size) {
      $candidate = [string]$details.parameter_size
    } elseif ($details.PSObject.Properties.Name -contains "size" -and $details.size) {
      $candidate = [string]$details.size
    }
  }

  if (-not $candidate -and $Model.PSObject.Properties.Name -contains "size" -and $Model.size) {
    $candidate = [string]$Model.size
  }

  if (-not $candidate) {
    return $null
  }

  $match = [regex]::Match($candidate, '(\d+(?:\.\d+)?)\s*B', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
  if ($match.Success) {
    return [double]::Parse($match.Groups[1].Value, [System.Globalization.CultureInfo]::InvariantCulture)
  }

  return $null
}

function Get-OllamaModelReason {
  param([object]$Model)

  $name = Get-OllamaModelName -Model $Model
  if (-not $name) {
    return "missing model name"
  }

  $normalized = $name.ToLowerInvariant()
  $reasons = New-Object System.Collections.Generic.List[string]
  $familyTokens = New-Object System.Collections.Generic.List[string]

  if ($Model.PSObject.Properties.Name -contains "family" -and $Model.family) {
    $familyTokens.Add(([string]$Model.family).ToLowerInvariant())
  }

  if ($Model.PSObject.Properties.Name -contains "families" -and $Model.families) {
    foreach ($family in @($Model.families)) {
      $token = [string]$family
      if ($token.Trim()) {
        $familyTokens.Add($token.Trim().ToLowerInvariant())
      }
    }
  }

  if ($Model.PSObject.Properties.Name -contains "details" -and $Model.details) {
    $details = $Model.details
    if ($details.PSObject.Properties.Name -contains "family" -and $details.family) {
      $familyTokens.Add(([string]$details.family).ToLowerInvariant())
    }
    if ($details.PSObject.Properties.Name -contains "families" -and $details.families) {
      foreach ($family in @($details.families)) {
        $token = [string]$family
        if ($token.Trim()) {
          $familyTokens.Add($token.Trim().ToLowerInvariant())
        }
      }
    }
  }

  if ($normalized -match '(embed|embedding)') {
    $reasons.Add("embedding model")
  }
  if ($normalized -match '(vision|-vl|:vl)') {
    $reasons.Add("vision or multimodal model")
  }
  if ($familyTokens | Where-Object { $_ -eq "mllama" -or $_ -like "*vision*" -or $_ -like "*vl" -or $_ -like "*vlmoe*" }) {
    if (-not ($reasons -contains "vision or multimodal model")) {
      $reasons.Add("vision or multimodal model")
    }
  }

  $sizeB = Get-OllamaModelSizeB -Model $Model
  if ($null -ne $sizeB -and $sizeB -gt 14) {
    $reasons.Add("larger than 14B")
  }

  if ($reasons.Count -eq 0) {
    return ""
  }

  return ($reasons -join ", ")
}

function Get-OllamaModelPriorityScore {
  param([object]$Model)

  $name = Get-OllamaModelName -Model $Model
  $normalized = $name.ToLowerInvariant()
  $score = 0

  if ($normalized -match '(embed|embedding|vision|-vl|:vl)') {
    return -1000
  }

  if ($normalized -match 'instruct') { $score += 40 }
  if ($normalized -match 'latest') { $score += 10 }
  if ($normalized -match 'coder') { $score += 8 }

  $sizeB = Get-OllamaModelSizeB -Model $Model
  if ($null -ne $sizeB) {
    if ($sizeB -ge 6 -and $sizeB -le 10) {
      $score += 30
    } elseif ($sizeB -gt 10 -and $sizeB -le 14) {
      $score += 24
    } elseif ($sizeB -ge 3 -and $sizeB -lt 6) {
      $score += 18
    } elseif ($sizeB -ge 1 -and $sizeB -lt 3) {
      $score += 10
    }
  }

  return $score
}

function Get-OllamaModelCatalog {
  param(
    [string]$BaseUrl = "http://127.0.0.1:11434",
    [int]$TimeoutSeconds = 3
  )

  $uri = "$($BaseUrl.TrimEnd('/'))/api/tags"
  try {
    $response = Invoke-RestMethod -Uri $uri -Method Get -TimeoutSec $TimeoutSeconds -ErrorAction Stop
    $items = @()
    if ($response.models) {
      $items = @($response.models)
    }

    $installed = New-Object System.Collections.Generic.List[string]
    $selectable = New-Object System.Collections.Generic.List[object]
    $filtered = New-Object System.Collections.Generic.List[object]

    foreach ($item in $items) {
      $name = Get-OllamaModelName -Model $item
      if (-not $name) {
        continue
      }

      $installed.Add($name)
      $reason = Get-OllamaModelReason -Model $item
      if ($reason) {
        $filtered.Add(@{ name = $name; reason = $reason })
      } else {
        $selectable.Add($item)
      }
    }

    $selectableSorted = @($selectable | Sort-Object -Stable -Descending -Property @{ Expression = { Get-OllamaModelPriorityScore -Model $_ } })
    $selectableNames = @($selectableSorted | ForEach-Object { Get-OllamaModelName -Model $_ })
    if (-not $selectableNames) {
      $selectableNames = @()
    }

    $selected = ""
    if ($selectableNames.Count -gt 0) {
      $selected = [string]$selectableNames[0]
    }

    return @{
      provider_contract_version = $script:OllamaProviderContractVersion
      enabled = $true
      reachable = $true
      provider = "ollama"
      base_url = $BaseUrl
      requested_model = $null
      requested_model_available = $null
      requested_model_installed = $null
      requested_model_selectable = $null
      installed_models = @($installed)
      models = @($selectableNames)
      filtered_models = @($filtered)
      selected_model = $selected
      error = ""
    }
  } catch {
    return @{
      provider_contract_version = $script:OllamaProviderContractVersion
      enabled = $true
      reachable = $false
      provider = "ollama"
      base_url = $BaseUrl
      requested_model = $null
      requested_model_available = $null
      requested_model_installed = $null
      requested_model_selectable = $null
      installed_models = @()
      models = @()
      filtered_models = @()
      selected_model = ""
      error = $_.Exception.Message
    }
  }
}

function Test-ObjectHasProperty {
  param(
    [object]$Object,
    [string]$Name
  )

  if ($null -eq $Object -or -not $Name) {
    return $false
  }

  if ($Object -is [System.Collections.IDictionary]) {
    return $Object.Contains($Name)
  }

  $properties = $Object.PSObject.Properties
  return [bool]($properties -and $properties.Name -contains $Name)
}

function Get-ObjectPropertyValue {
  param(
    [object]$Object,
    [string]$Name,
    [object]$Default = $null
  )

  if (-not (Test-ObjectHasProperty -Object $Object -Name $Name)) {
    return $Default
  }

  if ($Object -is [System.Collections.IDictionary]) {
    return $Object[$Name]
  }

  return $Object.$Name
}

function ConvertTo-NullableBool {
  param([object]$Value)

  if ($null -eq $Value) {
    return $null
  }

  if ($Value -is [bool]) {
    return [bool]$Value
  }

  $text = [string]$Value
  if ([string]::IsNullOrWhiteSpace($text)) {
    return $null
  }

  $normalized = $text.Trim().ToLowerInvariant()
  if ($normalized -eq "true") {
    return $true
  }
  if ($normalized -eq "false") {
    return $false
  }

  return $null
}

function ConvertTo-StringArray {
  param([object]$Value)

  if ($null -eq $Value) {
    return @()
  }

  $items = @()
  if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
    $items = @($Value)
  } else {
    $items = @($Value)
  }

  return @(
    $items |
      ForEach-Object { [string]$_ } |
      ForEach-Object { $_.Trim() } |
      Where-Object { $_ }
  )
}

function ConvertTo-OllamaFilteredModelArray {
  param([object]$Value)

  if ($null -eq $Value) {
    return @()
  }

  $items = @()
  if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
    $items = @($Value)
  } else {
    $items = @($Value)
  }

  $filtered = @()
  foreach ($item in $items) {
    if ($null -eq $item) {
      continue
    }
    $name = [string](Get-ObjectPropertyValue -Object $item -Name "name" -Default "")
    $reason = [string](Get-ObjectPropertyValue -Object $item -Name "reason" -Default "")
    if (-not $name.Trim()) {
      continue
    }
    $filtered += ,@{
      name = $name.Trim()
      reason = $reason.Trim()
    }
  }

  return @($filtered)
}

function Normalize-OllamaStatus {
  param(
    [object]$Status,
    [string]$DefaultBaseUrl = "http://127.0.0.1:11434"
  )

  $rawVersion = Get-ObjectPropertyValue -Object $Status -Name "provider_contract_version" -Default $null
  $contractVersion = $null
  if ($null -ne $rawVersion -and [string]$rawVersion) {
    $parsedVersion = 0
    if ([int]::TryParse(([string]$rawVersion), [ref]$parsedVersion)) {
      $contractVersion = $parsedVersion
    }
  }

  $selectedModel = [string](Get-ObjectPropertyValue -Object $Status -Name "selected_model" -Default "")
  $requestedModel = [string](Get-ObjectPropertyValue -Object $Status -Name "requested_model" -Default "")
  $errorText = [string](Get-ObjectPropertyValue -Object $Status -Name "error" -Default "")
  $baseUrl = [string](Get-ObjectPropertyValue -Object $Status -Name "base_url" -Default $DefaultBaseUrl)
  if (-not $baseUrl.Trim()) {
    $baseUrl = $DefaultBaseUrl
  }

  return @{
    provider_contract_version = $contractVersion
    enabled = $(
      $value = ConvertTo-NullableBool (Get-ObjectPropertyValue -Object $Status -Name "enabled" -Default $true)
      if ($null -eq $value) { $true } else { $value }
    )
    reachable = $(
      $value = ConvertTo-NullableBool (Get-ObjectPropertyValue -Object $Status -Name "reachable" -Default $false)
      if ($null -eq $value) { $false } else { $value }
    )
    provider = [string](Get-ObjectPropertyValue -Object $Status -Name "provider" -Default "ollama")
    base_url = $baseUrl.Trim()
    selected_model = $selectedModel.Trim()
    requested_model = $requestedModel.Trim()
    requested_model_available = ConvertTo-NullableBool (Get-ObjectPropertyValue -Object $Status -Name "requested_model_available" -Default $null)
    requested_model_installed = ConvertTo-NullableBool (Get-ObjectPropertyValue -Object $Status -Name "requested_model_installed" -Default $null)
    requested_model_selectable = ConvertTo-NullableBool (Get-ObjectPropertyValue -Object $Status -Name "requested_model_selectable" -Default $null)
    models = ConvertTo-StringArray (Get-ObjectPropertyValue -Object $Status -Name "models" -Default @())
    installed_models = ConvertTo-StringArray (Get-ObjectPropertyValue -Object $Status -Name "installed_models" -Default @())
    filtered_models = ConvertTo-OllamaFilteredModelArray (Get-ObjectPropertyValue -Object $Status -Name "filtered_models" -Default @())
    error = $errorText.Trim()
  }
}

function Get-OllamaProviderContractState {
  param([object]$Status)

  if ($null -eq $Status) {
    return "unusable"
  }

  $hasCurrentVersion = (Get-ObjectPropertyValue -Object $Status -Name "provider_contract_version" -Default $null) -eq $script:OllamaProviderContractVersion
  $requiredProperties = @(
    "provider_contract_version",
    "models",
    "installed_models",
    "filtered_models",
    "selected_model",
    "requested_model",
    "requested_model_available",
    "requested_model_installed",
    "requested_model_selectable"
  )

  $missingRequired = @(
    $requiredProperties |
      Where-Object { -not (Test-ObjectHasProperty -Object $Status -Name $_) }
  )

  if ($hasCurrentVersion -and $missingRequired.Count -eq 0) {
    return "current"
  }

  $legacySignals = @(
    "provider",
    "base_url",
    "models",
    "selected_model",
    "error"
  )

  $hasLegacySignals = @(
    $legacySignals |
      Where-Object { Test-ObjectHasProperty -Object $Status -Name $_ }
  ).Count -gt 0

  if ($hasLegacySignals) {
    return "stale"
  }

  return "unusable"
}

function Format-OllamaModelLine {
  param(
    [string]$Prefix,
    [string]$Name,
    [string]$Reason = ""
  )

  if ($Reason) {
    return "$Prefix $Name ($Reason)"
  }

  return "$Prefix $Name"
}

function Get-ListeningProcessId {
  param([int]$Port)

  $connection = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($connection) {
    return [int]$connection.OwningProcess
  }

  $match = netstat -ano | Select-String -Pattern "[:.]$Port\s+.*LISTENING\s+(\d+)$" | Select-Object -First 1
  if ($match) {
    $groups = [regex]::Match($match.Line, "(\d+)\s*$")
    if ($groups.Success) {
      return [int]$groups.Groups[1].Value
    }
  }

  return $null
}

function Wait-PortFree {
  param(
    [int]$Port,
    [int]$TimeoutSeconds = 10
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    if (-not (Test-PortInUse -Port $Port)) {
      return $true
    }
    Start-Sleep -Milliseconds 500
  }

  return -not (Test-PortInUse -Port $Port)
}

function Stop-HcDataBackendOnPort {
  param(
    [int]$Port,
    [object]$Health,
    [int]$TimeoutSeconds = 10
  )

  $serviceName = [string](Get-ObjectPropertyValue -Object $Health -Name "service" -Default "")
  if ($serviceName -ne "hc-data-cleanup-ai") {
    return @{
      stopped = $false
      pid = $null
      message = "Port $Port is not confirmed as hc-data-cleanup-ai."
    }
  }

  $processId = Get-ListeningProcessId -Port $Port
  if ($null -eq $processId) {
    return @{
      stopped = $true
      pid = $null
      message = "Port $Port was already free."
    }
  }

  try {
    Stop-Process -Id $processId -Force -ErrorAction Stop
  } catch {
    return @{
      stopped = $false
      pid = $processId
      message = "Failed to stop the stale hc-data-cleanup-ai backend on port ${Port}: $($_.Exception.Message)"
    }
  }

  if (Wait-PortFree -Port $Port -TimeoutSeconds $TimeoutSeconds) {
    return @{
      stopped = $true
      pid = $processId
      message = "Stopped stale hc-data-cleanup-ai backend on port $Port (PID $processId)."
    }
  }

  return @{
    stopped = $false
    pid = $processId
    message = "Requested stop for stale hc-data-cleanup-ai backend on port $Port (PID $processId), but the port did not free within $TimeoutSeconds seconds."
  }
}

function Ensure-Venv {
  param([string]$RepoRoot)

  $venvPath = Get-VenvPath -RepoRoot $RepoRoot
  $pythonExe = Get-VenvPythonPath -RepoRoot $RepoRoot
  if (Test-Path -LiteralPath $pythonExe) {
    return $pythonExe
  }

  $systemPython = Get-SystemPythonCommand
  $arguments = @()
  $arguments += $systemPython.Prefix
  $arguments += "-m", "venv", $venvPath

  Write-Host "Creating virtual environment at $venvPath"
  & $systemPython.Command @arguments | Out-Host
  if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $pythonExe)) {
    throw "Failed to create virtual environment."
  }

  return $pythonExe
}

function Ensure-BackendDependencies {
  param([string]$RepoRoot)

  $pythonExe = Ensure-Venv -RepoRoot $RepoRoot
  $requirementsPath = Join-Path $RepoRoot "backend\requirements.txt"
  $markerPath = Join-Path (Get-VenvPath -RepoRoot $RepoRoot) "backend_requirements.sha256"
  $requiredHash = Get-FileHashHex -Path $requirementsPath
  $installedHash = ""

  if (Test-Path -LiteralPath $markerPath) {
    $installedHash = (Get-Content -LiteralPath $markerPath -ErrorAction SilentlyContinue | Select-Object -First 1)
  }

  if ($requiredHash -ne $installedHash) {
    Write-Host "Installing backend dependencies"
    & $pythonExe -m pip install -r $requirementsPath | Out-Host
    if ($LASTEXITCODE -ne 0) {
      throw "Dependency installation failed."
    }
    Set-Content -LiteralPath $markerPath -Encoding UTF8 -Value $requiredHash
  }

  return $pythonExe
}

function Write-LauncherState {
  param(
    [string]$RepoRoot,
    [hashtable]$State
  )

  $statePath = Get-LauncherStatePath -RepoRoot $RepoRoot
  $stateDir = Split-Path -Parent $statePath
  if (-not (Test-Path -LiteralPath $stateDir)) {
    New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
  }

  $payload = [ordered]@{
    updated_at = (Get-Date).ToString("o")
    base_url = [string](Get-ObjectPropertyValue -Object $State -Name "base_url" -Default "")
    port = [int](Get-ObjectPropertyValue -Object $State -Name "port" -Default 0)
    workflow_version = [string](Get-ObjectPropertyValue -Object $State -Name "workflow_version" -Default "")
    backend_status = [string](Get-ObjectPropertyValue -Object $State -Name "backend_status" -Default "")
    ollama_reachable = ConvertTo-NullableBool (Get-ObjectPropertyValue -Object $State -Name "ollama_reachable" -Default $null)
  }

  $payload | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statePath -Encoding UTF8
}

function Read-LauncherState {
  param([string]$RepoRoot)

  $statePath = Get-LauncherStatePath -RepoRoot $RepoRoot
  if (-not (Test-Path -LiteralPath $statePath)) {
    return $null
  }

  try {
    return Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
  } catch {
    return $null
  }
}

function Initialize-BackendEnvironment {
  param(
    [string]$RepoRoot,
    [string]$WorkflowVersion = "v3_guided"
  )

  $env:PYTHONPATH = Join-Path $RepoRoot "backend"
  $env:UI_WORKFLOW_VERSION = if ($WorkflowVersion) { $WorkflowVersion } else { "v3_guided" }
  if (-not $env:ALLOWED_ORIGINS) {
    $env:ALLOWED_ORIGINS = "*"
  }
  if (-not $env:SESSION_SECRET_KEY) {
    $env:SESSION_SECRET_KEY = "dev-session-secret-change-me"
  }
}

function Test-PortInUse {
  param([int]$Port)

  $connection = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
  if ($connection) {
    return $true
  }

  $matches = netstat -ano | Select-String -Pattern "[:.]$Port\s+.*LISTENING"
  return $null -ne $matches
}

function Find-AvailablePort {
  param(
    [int]$PreferredPort = 8000,
    [int]$MaxOffset = 20
  )

  if (-not (Test-PortInUse -Port $PreferredPort)) {
    return $PreferredPort
  }

  for ($offset = 1; $offset -le $MaxOffset; $offset++) {
    $candidate = $PreferredPort + $offset
    if (-not (Test-PortInUse -Port $candidate)) {
      return $candidate
    }
  }

  throw "No free port found between $PreferredPort and $($PreferredPort + $MaxOffset)."
}

function Invoke-ApiGetJson {
  param(
    [string]$Uri,
    [int]$TimeoutSeconds = 3
  )

  try {
    return Invoke-RestMethod -Uri $Uri -Method Get -TimeoutSec $TimeoutSeconds -ErrorAction Stop
  } catch {
    return $null
  }
}

function Wait-ApiHealth {
  param(
    [string]$BaseUrl,
    [int]$TimeoutSeconds = 60
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  $healthUri = "$($BaseUrl.TrimEnd('/'))/api/health"

  while ((Get-Date) -lt $deadline) {
    $health = Invoke-ApiGetJson -Uri $healthUri -TimeoutSeconds 3
    $isExpectedHealth = $health `
      -and $health.PSObject.Properties.Name -contains "service" `
      -and $health.service -eq "hc-data-cleanup-ai"
    if ($isExpectedHealth) {
      return $health
    }
    Start-Sleep -Milliseconds 1000
  }

  return $null
}

function Get-OllamaStatus {
  param(
    [string]$BaseUrl = "http://127.0.0.1:11434",
    [int]$TimeoutSeconds = 3
  )
  return Get-OllamaModelCatalog -BaseUrl $BaseUrl -TimeoutSeconds $TimeoutSeconds
}
