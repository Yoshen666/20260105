@set disk=%~d0
@%disk%
set build_path=%~dp0
cd %build_path%

@REM call conda activate etl
@REM call python ETL.py install
call python ETL.py start
pause