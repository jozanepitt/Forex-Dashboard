# service-guardian.ps1
# Keeps the forex signal service alive. Run by the "ForexSignalService" scheduled
# task at logon and every few minutes. If port 3002 is not listening, it relaunches
# app.py DETACHED (via WMI Win32_Process.Create) so the service is owned by the WMI
# host, not by this script or the Task Scheduler job — it survives session/task end.
$ErrorActionPreference = "Stop"

$dir = "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
$pyw = "C:\Users\jzpit\AppData\Local\Programs\Python\Python312\pythonw.exe"
$log = Join-Path $dir "guardian.log"

function Write-Log($msg) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -Path $log -Value "$ts  $msg" -Encoding utf8
}

try {
    $up = Get-NetTCPConnection -LocalPort 3002 -State Listen -ErrorAction SilentlyContinue
    if ($up) {
        # Healthy — listening. Stay quiet (no log spam every 3 min).
        exit 0
    }

    # Debounce: a single "not listening" snapshot can just be the brief gap of a
    # manual restart already in progress. Re-check after a few seconds before
    # acting -- a genuinely dead service is still down on the second check too.
    Start-Sleep -Seconds 5
    $up = Get-NetTCPConnection -LocalPort 3002 -State Listen -ErrorAction SilentlyContinue
    if ($up) { exit 0 }

    # Not listening: clear any zombie app.py process, then verify it's actually gone
    # before relaunching. A silently-failed kill here previously left the old process
    # alive alongside a freshly relaunched one, doubling every scheduled job (and
    # every Discord alert).
    $zombies = Get-CimInstance Win32_Process -Filter "Name='pythonw.exe' OR Name='python.exe'" |
        Where-Object { $_.CommandLine -like "*app.py*" }

    $blocked = $false
    foreach ($z in $zombies) {
        try {
            Stop-Process -Id $z.ProcessId -Force -ErrorAction Stop
            Write-Log "killed zombie app.py (pid $($z.ProcessId))"
        } catch {
            Write-Log "FAILED to kill zombie app.py (pid $($z.ProcessId)): $($_.Exception.Message)"
            $blocked = $true
        }
    }

    if ($zombies) {
        Start-Sleep -Milliseconds 1500   # give Windows a moment to fully release the process
        foreach ($z in $zombies) {
            if (Get-Process -Id $z.ProcessId -ErrorAction SilentlyContinue) {
                Write-Log "zombie app.py (pid $($z.ProcessId)) still alive after kill - skipping relaunch this run"
                $blocked = $true
            }
        }
    }

    if (-not $blocked) {
        $cmd = "`"$pyw`" app.py"
        $r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $cmd; CurrentDirectory = $dir }
        Write-Log "service was DOWN -> relaunched (Win32_Process.Create rc=$($r.ReturnValue) pid=$($r.ProcessId))"
    }
}
catch {
    Write-Log "guardian error: $($_.Exception.Message)"
    exit 1
}
