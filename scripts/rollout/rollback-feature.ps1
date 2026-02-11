param(
  [Parameter(Mandatory = $true)]
  [string]$FeatureId
)

$ErrorActionPreference = "Stop"

function Ensure-GitRepo {
  $isRepo = git rev-parse --is-inside-work-tree 2>$null
  if ($LASTEXITCODE -ne 0 -or "$isRepo" -ne "true") {
    throw "Current folder is not a git repository."
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
$rollbackBranch = "rollback/$($feature.ToLower())"

$exists = git tag --list $preTag
if (-not $exists) {
  throw "Pre tag not found: $preTag"
}

git switch -C $rollbackBranch $preTag

Write-Host "Rollback branch created: $rollbackBranch"
Write-Host "Pointing to tag: $preTag"

