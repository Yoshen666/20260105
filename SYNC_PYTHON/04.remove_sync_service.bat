@set disk=%~d0
@%disk%
set build_path=%~dp0
cd %build_path%

@REM call conda activate etl
call python ETL.py stop
call python ETL.py remove
pause