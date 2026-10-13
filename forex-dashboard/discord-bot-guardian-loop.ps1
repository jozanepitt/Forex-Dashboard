# discord-bot-guardian-loop.ps1
# Keeps discord_bot.py (the /analyse slash-command bot) alive WITHOUT admin
# rights. Mirrors service-guardian-loop.ps1's design for app.py, adapted for
# a process that doesn't listen on a TCP port: liveness is "is a pythonw/
# python process running discord_bot.py present", not "is a port listening".
#
# Launched hidden at logon by the Startup-folder shortcut (DiscordBotGuardian.vbs),
# and also started immediately when first installed. Loops every 3 minutes.
#
# Single-instance: a global mutex guarantees only ONE loop ever runs, even if
# the Startup launcher fires while a copy is already active.
$ErrorActionPreference = "Stop"

$mutex = New-Object System.Threading.Mutex($false, "Global\DiscordBotGuardianLoop")
if (-not $mutex.WaitOne(0)) { exit 0 }   # another guardian loop already running

$dir = "C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service"
$pyw = "C:\Users\jzpit\AppData\Local\Programs\Python\Python312\pythonw.exe"
$py = "C:\Users\jzpit\AppData\Local\Programs\Python\Python312\python.exe"
$log = Join-Path $dir "discord_bot_guardian.log"

function Write-Log($msg) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -Path $log -Value "$ts  $msg" -Encoding utf8
}

Write-Log "guardian loop started (pid $PID)"

while ($true) {
    try {
        $running = Get-CimInstance Win32_Process -Filter "Name='pythonw.exe' OR Name='python.exe'" |
            Where-Object { $_.CommandLine -like "*discord_bot.py*" }

        if (-not $running) {
            # DISCORD_BOT_TOKEN isn't set yet until JZ creates the bot in the
            # Discord Developer Portal -- without this check the loop would
            # spawn a process that immediately exits, every 3 minutes, forever.
            # Reuses config.py's own env loading (never reads/prints the value).
            & $py -c "from config import DISCORD_BOT_TOKEN; import sys; sys.exit(0 if DISCORD_BOT_TOKEN else 1)" 2>$null
            if ($LASTEXITCODE -ne 0) {
                Write-Log "DISCORD_BOT_TOKEN not set in .env -- skipping relaunch until it is added"
            } else {
                $r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = "`"$pyw`" discord_bot.py"; CurrentDirectory = $dir }
                Write-Log "discord_bot.py was DOWN -> relaunched (rc=$($r.ReturnValue) pid=$($r.ProcessId))"
            }
        }
    }
    catch {
        Write-Log "loop error: $($_.Exception.Message)"
    }
    Start-Sleep -Seconds 180
}
