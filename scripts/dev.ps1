param(
  [int]$Port = 8000
)

function Test-PortInUse {
  param([int]$TargetPort)
  $matches = netstat -ano | Select-String -Pattern ":$TargetPort\s+.*LISTENING"
  return $null -ne $matches
}

$repoRoot = Resolve-Path "$PSScriptRoot\.."
$venvPath = Join-Path $repoRoot ".venv"
$pythonExe = Join-Path $venvPath "Scripts\python.exe"
$reqFile = Join-Path $repoRoot "backend\requirements.txt"
$markerFile = Join-Path $repoRoot "backend\.deps_installed"

function Get-FileHashHex($path) {
  if (-not (Test-Path $path)) { return "" }
  return (Get-FileHash -Algorithm SHA256 $path).Hash
}

if (-not (Test-Path $pythonExe)) {
  Write-Host "Creating virtual environment..."
  python -m venv $venvPath
}

$reqHash = Get-FileHashHex $reqFile
$installedHash = ""
if (Test-Path $markerFile) {
  $installedHash = Get-Content $markerFile -ErrorAction SilentlyContinue
}

if ($reqHash -ne $installedHash) {
  Write-Host "Installing backend dependencies..."
  & $pythonExe -m pip install -r $reqFile
  $reqHash | Set-Content -Encoding UTF8 $markerFile
} else {
  Write-Host "Dependencies already installed."
}

$originalPort = $Port
if (Test-PortInUse $Port) {
  for ($i = 1; $i -le 10; $i++) {
    $candidate = $originalPort + $i
    if (-not (Test-PortInUse $candidate)) {
      $Port = $candidate
      break
    }
  }
  if ($Port -ne $originalPort) {
    Write-Host "Port $originalPort in use. Using $Port instead."
  } else {
    Write-Host "Port $originalPort in use and no free port found."
    exit 1
  }
}

$env:PYTHONPATH = "$repoRoot\backend"
Push-Location $repoRoot
& $pythonExe -m uvicorn app.main:app --reload --host 0.0.0.0 --port $Port
Pop-Location
