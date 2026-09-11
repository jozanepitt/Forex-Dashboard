# service-guardian-loop.ps1
# Keeps the forex signal service (app.py, port 3002) alive WITHOUT admin rights.
# Launched hidden at logon by the Startup-folder shortcut, and also started
# immediately when first installed. Loops every 3 minutes: if port 3002 is not
# listening, it relaunches app.py DETACHED via WMI (Win32_Process.Create) so the
# service is owned by the WMI host and survives this loop, the logon session, etc.
#
# Single-instance: a global mutex guarantees only ONE loop ever runs, even if the
# Startup launcher fires while a copy is already active.
$ErrorActionPreference = "Stop"

$mutex = New-Object System.Threading.Mutex($false, "Global\ForexSignalGuardianLoop")
if (-not $mutex.WaitOne(0)) { exit 0 }   # another guardian loop already running

$dir = "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
$pyw = "C:\Users\jzpit\AppData\Local\Programs\Python\Python312\pythonw.exe"
$log = Join-Path $dir "guardian.log"

function Write-Log($msg) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -Path $log -Value "$ts  $msg" -Encoding utf8
}

Write-Log "guardian loop started (pid $PID)"

while ($true) {
    try {
        $up = Get-NetTCPConnection -LocalPort 3002 -State Listen -ErrorAction SilentlyContinue
        if (-not $up) {
            # Clear any zombie app.py, then verify each one is actually gone before relaunching.
            # A silently-failed kill here previously left the old process alive alongside a
            # freshly relaunched one, doubling every scheduled job (and every Discord alert).
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
                        Write-Log "zombie app.py (pid $($z.ProcessId)) still alive after kill - skipping relaunch this cycle"
                        $blocked = $true
                    }
                }
            }

            if ($blocked) {
                # Don't stack a new process on top of one we couldn't confirm dead;
                # the next loop iteration (3 min) will retry the kill.
            } else {
                $r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = "`"$pyw`" app.py"; CurrentDirectory = $dir }
                Write-Log "service was DOWN -> relaunched (rc=$($r.ReturnValue) pid=$($r.ProcessId))"
            }
        }
    }
    catch {
        Write-Log "loop error: $($_.Exception.Message)"
    }
    Start-Sleep -Seconds 180
}
