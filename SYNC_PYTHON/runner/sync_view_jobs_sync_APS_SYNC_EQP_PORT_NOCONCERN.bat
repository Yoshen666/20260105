setlocal enabledelayedexpansion
cd /d %~d0
cd /d %~dp0
set "filename=%~nx0"
set "name=!filename:.bat=!"
set pythonfile=%name%.py

start /B python %pythonfile%
            
