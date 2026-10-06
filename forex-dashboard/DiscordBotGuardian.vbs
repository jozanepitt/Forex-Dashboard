' DiscordBotGuardian.vbs  (repo copy for reproducibility)
' Deploy by copying this file to:
'   %APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\DiscordBotGuardian.vbs
' It launches the guardian loop HIDDEN at logon (no console window). The guardian
' keeps discord_bot.py (the /analyse slash-command bot) running and relaunches
' it detached if it dies. Mirrors ForexSignalGuardian.vbs's role for app.py.
Dim sh
Set sh = CreateObject("WScript.Shell")
sh.Run "powershell.exe -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File ""C:\Users\jzpit\OneDrive\Documents\OpenCode\forex-dashboard\discord-bot-guardian-loop.ps1""", 0, False
