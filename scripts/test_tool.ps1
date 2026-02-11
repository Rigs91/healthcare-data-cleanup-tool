param(
  [string]$BaseUrl = "http://localhost:8000",
  [string]$SampleFile = ""
)

$repoRoot = Resolve-Path "$PSScriptRoot\.."
if (-not $SampleFile) {
  $SampleFile = Join-Path $repoRoot "data\sample_messy.csv"
}

Write-Host "1) Health check"
$health = curl.exe -s "$BaseUrl/api/health" | ConvertFrom-Json
$health | Format-List

Write-Host "2) Upload sample dataset"
$uploadJson = curl.exe -s -X POST "$BaseUrl/api/datasets" -F "file=@$SampleFile" -F "name=Sample messy dataset" -F "usage_intent=training" -F "output_format=csv" -F "privacy_mode=safe_harbor"
$dataset = $uploadJson | ConvertFrom-Json
Write-Host "Dataset ID: $($dataset.id)"

Write-Host "3) Run cleaning"
$options = @{ remove_duplicates = $true; drop_empty_columns = $true; deidentify = $false } | ConvertTo-Json
$cleanJson = curl.exe -s -X POST "$BaseUrl/api/datasets/$($dataset.id)/clean" -H "Content-Type: application/json" -d $options
$cleaned = $cleanJson | ConvertFrom-Json
$cleaned.qc | ConvertTo-Json -Depth 5

Write-Host "4) Download cleaned dataset"
$downloadPath = Join-Path $repoRoot "data\cleaned\demo_cleaned.csv"
curl.exe -s -o $downloadPath "$BaseUrl/api/datasets/$($dataset.id)/download?kind=cleaned"
Write-Host "Saved cleaned file to: $downloadPath"
