$ErrorActionPreference = "Stop"

$OS_URL = $env:OS_URL
if (-not $OS_URL) { $OS_URL = "http://localhost:9200" }


# Upload index templates
Invoke-RestMethod -Uri "$OS_URL/_index_template/trending-1m" -Method PUT -ContentType "application/json" -InFile "conf/opensearch/index-templates/trending-1m.json"
Invoke-RestMethod -Uri "$OS_URL/_index_template/trending-5m" -Method PUT -ContentType "application/json" -InFile "conf/opensearch/index-templates/trending-5m.json"


# Create today's indices and aliases
$today = Get-Date -Format "yyyy.MM.dd"
$prefixes = @("trending-1m", "trending-5m")
foreach ($pfx in $prefixes) {
    $index = "$pfx-$today"
    $body = @{ aliases = @{ $pfx = @{} } } | ConvertTo-Json -Compress
    Invoke-RestMethod -Uri "$OS_URL/$index" -Method PUT -ContentType "application/json" -Body $body
}