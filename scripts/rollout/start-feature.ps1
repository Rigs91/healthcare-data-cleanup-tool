param(
  [Parameter(Mandatory = $true)]
  [string]$FeatureId,
  [Parameter(Mandatory = $false)]
  [string]$Title = ""
)

$ErrorActionPreference = "Stop"

function Ensure-GitRepo {
  $isRepo = git rev-parse --is-inside-work-tree 2>$null
  if ($LASTEXITCODE -ne 0 -or "$isRepo" -ne "true") {
    throw "Current folder is not a git repository. Initialize git first."
  }
}

function Normalize-FeatureId([string]$value) {
  $normalized = $value.Trim().ToUpper()
  if ($normalized -notmatch "^F\d{2}$") {
    throw "FeatureId must match Fxx (example: F01)."
  }
  return $normalized
}

Ensure-GitRepo
$feature = Normalize-FeatureId $FeatureId
$preTag = "pre-$($feature.ToLower())"

git tag --list $preTag | Out-Null
if (-not (git tag --list $preTag)) {
  git tag $preTag
}

$slug = if ([string]::IsNullOrWhiteSpace($Title)) { "work" } else { $Title.ToLower() -replace "[^a-z0-9]+", "-" }
$slug = $slug.Trim("-")
if ([string]::IsNullOrWhiteSpace($slug)) { $slug = "work" }

$branch = "rollout/$($feature.ToLower())-$slug"
git switch -c $branch

$signoffPath = "docs/rollout/signoffs/$feature.md"
if (-not (Test-Path $signoffPath)) {
  $template = Get-Content "docs/rollout/signoffs/TEMPLATE.md" -Raw
  $template = $template -replace "Feature ID:", "Feature ID: $feature"
  $template = $template -replace "Title:", "Title: $Title"
  $template = $template -replace "Branch:", "Branch: $branch"
  Set-Content -Path $signoffPath -Value $template -Encoding UTF8
}

Write-Host "Started $feature"
Write-Host "Branch: $branch"
Write-Host "Pre-tag: $preTag"
Write-Host "Signoff: $signoffPath"

