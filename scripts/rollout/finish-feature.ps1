param(
  [Parameter(Mandatory = $true)]
  [string]$FeatureId,
  [Parameter(Mandatory = $false)]
  [string]$CommitMessage = ""
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
$postTag = "post-$($feature.ToLower())"

if ([string]::IsNullOrWhiteSpace($CommitMessage)) {
  $CommitMessage = "feat($($feature.ToLower())): complete rollout feature $feature"
}

git add -A
git commit -m $CommitMessage

git tag --list $postTag | Out-Null
if (-not (git tag --list $postTag)) {
  git tag $postTag
}

Write-Host "Finished $feature"
Write-Host "Post-tag: $postTag"

