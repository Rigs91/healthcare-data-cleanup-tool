$repoRoot = Resolve-Path "$PSScriptRoot\.."
$env:PYTHONPATH = "$repoRoot\backend"
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"

Push-Location $repoRoot
& $pythonExe -m pytest backend\tests -q
Pop-Location
