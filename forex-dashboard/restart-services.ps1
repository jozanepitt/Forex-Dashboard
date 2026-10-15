# restart-services.ps1
# Full restart behind the dashboard's "Restart" button (POST /admin/restart).
# app.py launches this detached and then gets killed by it, so it must not
# depend on app.py staying alive. Order matters:
#   1. stop both guardian loops first -- otherwise the service guardian sees
#      port 3002 go quiet mid-restart and kills the fresh app.py as a zombie
#   2. stop app.py and discord_bot.py, and confirm they are gone
#   3. start app.py and wait for /health to answer from the NEW pid
#   4. start discord_bot.py (when its token is set)
#   5. start the guardians again, only now that app.py is answering
# Every step goes to service\guardian.log with a "restart:" prefix.
param([int]$OldPid = 0)
$ErrorActionPreference = "Continue"

$mutex = New-Object System.Threading.Mutex($false, "Global\ForexSignalRestart")
if (-not $mutex.WaitOne(0)) { exit 0 }   # a restart is already running

$root = "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard"
$dir = Join-Path $root "service"
$pyw = "C:\Users\jzpit\AppData\Local\Programs\Python\Python312\pythonw.exe"
$py = "C:\Users\jzpit\AppData\Local\Programs\Python\Python312\python.exe"
$log = Join-Path $dir "guardian.log"

function Write-Log($msg) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -Path $log -Value "$ts  restart: $msg" -Encoding utf8
}

function Get-Procs($exeFilter, $pattern) {
    Get-CimInstance Win32_Process -Filter $exeFilter | Where-Object { $_.CommandLine -like $pattern }
}

# Returns $true when everything stopped, $false if any process survived.
function Stop-AndConfirm($procs, $label) {
    $allGone = $true
    foreach ($p in $procs) {
        try { Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop; Write-Log "stopped $label (pid $($p.ProcessId))" }
        catch { Write-Log "could not stop $label (pid $($p.ProcessId)): $($_.Exception.Message)" }
    }
    foreach ($p in $procs) {
        $deadline = (Get-Date).AddSeconds(15)
        while ((Get-Process -Id $p.ProcessId -ErrorAction SilentlyContinue) -and (Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 300
        }
        if (Get-Process -Id $p.ProcessId -ErrorAction SilentlyContinue) {
            Write-Log "$label (pid $($p.ProcessId)) is STILL running"
            $allGone = $false
        }
    }
    return $allGone
}

function Start-Detached($cmd, $cwd) {
    $r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $cmd; CurrentDirectory = $cwd }
    return $r
}

$t0 = Get-Date
Write-Log "requested from the dashboard (old app.py pid $OldPid)"

# 1. guardians
$guardians = Get-Procs "Name='powershell.exe'" "*guardian-loop.ps1*"
$null = Stop-AndConfirm $guardians "guardian loop"

# 2. services. Match `"...pythonw.exe" app.py` exactly, not any *app.py*
# (that would also hit e.g. a pytest run of test_app.py).
$pyFilter = "Name='pythonw.exe' OR Name='python.exe'"
$appGone = Stop-AndConfirm (Get-Procs $pyFilter '*" app.py') "app.py"
$botGone = Stop-AndConfirm (Get-Procs $pyFilter '*" discord_bot.py') "discord_bot.py"

if (-not $appGone) {
    # Never start a second app.py next to one that would not die: two copies
    # double every scheduled job and Discord alert. The guardian (restarted
    # below) retries the kill on its next cycle, as it always has.
    Write-Log "old app.py would not stop -- NOT starting a second copy; handing over to the guardian"
} else {
    # 3. app.py
    $r = Start-Detached "`"$pyw`" app.py" $dir
    Write-Log "started app.py (rc=$($r.ReturnValue) pid=$($r.ProcessId))"
    $healthy = $false
    $deadline = (Get-Date).AddSeconds(90)
    while ((Get-Date) -lt $deadline) {
        try {
            $h = Invoke-RestMethod -Uri "http://127.0.0.1:3002/health" -TimeoutSec 3
            if ($h.status -eq "ok" -and $h.pid -eq $r.ProcessId) { $healthy = $true; break }
        } catch { }
        Start-Sleep -Seconds 1
    }
    if ($healthy) { Write-Log "app.py is answering /health (pid $($h.pid)) after $([int]((Get-Date) - $t0).TotalSeconds)s" }
    else { Write-Log "app.py did NOT answer /health within 90s -- the service guardian will take over" }
}

# 4. Discord bot (same token check as discord-bot-guardian-loop.ps1; never prints the value)
Push-Location $dir
& $py -c "from config import DISCORD_BOT_TOKEN; import sys; sys.exit(0 if DISCORD_BOT_TOKEN else 1)" 2>$null
$hasToken = ($LASTEXITCODE -eq 0)
Pop-Location
if (-not $botGone) {
    Write-Log "old discord_bot.py would not stop -- NOT starting a second copy"
} elseif ($hasToken) {
    $b = Start-Detached "`"$pyw`" discord_bot.py" $dir
    Write-Log "started discord_bot.py (rc=$($b.ReturnValue) pid=$($b.ProcessId))"
} else {
    Write-Log "DISCORD_BOT_TOKEN not set -- Discord bot left off"
}

# 5. guardians back on (each has its own single-instance mutex)
foreach ($vbs in "ForexSignalGuardian.vbs", "DiscordBotGuardian.vbs") {
    $g = Start-Detached "wscript.exe `"$(Join-Path $root $vbs)`"" $root
    Write-Log "started $vbs (rc=$($g.ReturnValue))"
}
Write-Log "done in $([int]((Get-Date) - $t0).TotalSeconds)s"
