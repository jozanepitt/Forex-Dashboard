' ForexSignalGuardian.vbs  (repo copy for reproducibility)
' Deploy by copying this file to:
'   %APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\ForexSignalGuardian.vbs
' It launches the guardian loop HIDDEN at logon (no console window). The guardian
' keeps app.py (port 3002) running and relaunches it detached if it dies.
Dim sh
Set sh = CreateObject("WScript.Shell")
sh.Run "powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File ""C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\service-guardian-loop.ps1""", 0, False
