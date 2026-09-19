# bom_hcs_hourly.ps1 - hourly BoM HCS collection and current-readings publish.
#
# Runs from the scheduled task "Pluvio Stormgauge BoM HCS Hourly" (see README).
#   1. collect_bom_hcs.py collect        preserve new IDZ65900 snapshots from ftp.bom.gov.au
#   2. publish_bom_current_readings.py   rebuild bom_current_readings.json from the archive
#   3. commit and push that one file to origin/main when it changed (rebase over the radar
#      bot commits first; never force). Only bom_current_readings.json is ever added.
#
# Stop it:   Disable-ScheduledTask -TaskName 'Pluvio Stormgauge BoM HCS Hourly'
# Remove it: Unregister-ScheduledTask -TaskName 'Pluvio Stormgauge BoM HCS Hourly' -Confirm:$false
# Pause only the FTP collection: create the file source\bom_hcs_archive\STOP
# Logs: outputs\hcs_hourly\<yyyyMMdd_HHmmss>.log (outputs\ is gitignored)

param(
    [switch]$SkipCollect,
    [switch]$SkipPush
)

$ErrorActionPreference = 'Continue'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$LogRoot = Join-Path $ProjectRoot 'outputs\hcs_hourly'
New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null
$Stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$LogPath = Join-Path $LogRoot "$Stamp.log"
$Published = 'bom_current_readings.json'

function Log([string]$Text) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz') $Text"
    Add-Content -Path $LogPath -Value $line -Encoding ascii
    # Write-Host, not Write-Output: Run() returns its exit code through the output stream.
    Write-Host $line
}

function Run([string]$Label, [string[]]$Command) {
    Log "START $Label : $($Command -join ' ')"
    $output = & $Command[0] $Command[1..($Command.Length - 1)] 2>&1
    $code = $LASTEXITCODE
    foreach ($o in $output) { Add-Content -Path $LogPath -Value ("  " + $o) -Encoding ascii }
    Log "END   $Label exit=$code"
    return $code
}

Push-Location $ProjectRoot
try {
    Log "manifest: task=bom_hcs_hourly user=$env:USERNAME host=$env:COMPUTERNAME root=$ProjectRoot python=$((& py --version 2>&1) -join '') git=$((& git --version 2>&1) -join '')"
    if (-not $SkipCollect) {
        $c = Run 'collect' @('py', 'scripts\collect_bom_hcs.py', 'collect')
        if ($c -ne 0) { Log "WARN collect failed (exit $c); publishing from the existing archive" }
    }
    $p = Run 'publish' @('py', 'scripts\publish_bom_current_readings.py')
    if ($p -ne 0) { Log "FAIL publish exit=$p"; exit 2 }

    $changed = (& git status --porcelain -- $Published)
    if (-not $changed) { Log "no change to $Published; nothing to push"; exit 0 }
    if ($SkipPush) { Log "SkipPush set; $Published changed but not committed"; exit 0 }

    $steps = @(
        @('add', @('git', 'add', '--', $Published)),
        @('commit', @('git', 'commit', '-q', '-m', "bom_current_readings: publish ($((Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mmZ')))")),
        @('pull', @('git', 'pull', '-q', '--rebase', '--autostash', 'origin', 'main')),
        @('push', @('git', 'push', '-q', 'origin', 'main'))
    )
    foreach ($s in $steps) {
        $code = Run $s[0] $s[1]
        if ($code -ne 0) {
            Log "FAIL $($s[0]) exit=$code"
            if ($s[0] -eq 'pull') { & git rebase --abort 2>&1 | Out-Null; Log "rebase aborted; local commit kept for the next run" }
            exit 3
        }
    }
    Log "published $Published"
    exit 0
} finally {
    Pop-Location
}
