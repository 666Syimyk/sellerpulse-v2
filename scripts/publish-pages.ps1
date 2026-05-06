$ErrorActionPreference = "Stop"

if (-not $env:VITE_API_URL) {
  throw "Set VITE_API_URL before running this script. Example: `$env:VITE_API_URL='https://sellerpulse-api.onrender.com'"
}

$root = Split-Path -Parent $PSScriptRoot
$frontend = Join-Path $root "frontend"
$docs = Join-Path $root "docs"

Push-Location $frontend
try {
  $env:VITE_BASE = "./"
  npm.cmd run build:pages
} finally {
  Remove-Item Env:\VITE_BASE -ErrorAction SilentlyContinue
  Pop-Location
}

if (Test-Path $docs) {
  Remove-Item -Recurse -Force $docs
}

New-Item -ItemType Directory -Path $docs | Out-Null
Copy-Item -Path (Join-Path $frontend "dist\*") -Destination $docs -Recurse
New-Item -ItemType File -Path (Join-Path $docs ".nojekyll") -Force | Out-Null
