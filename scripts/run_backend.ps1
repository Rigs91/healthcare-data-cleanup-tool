param(
  [int]$Port = 8000
)

$repoRoot = Resolve-Path "$PSScriptRoot\.."
$env:PYTHONPATH = "$repoRoot\backend"

Push-Location $repoRoot
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port $Port
Pop-Location
