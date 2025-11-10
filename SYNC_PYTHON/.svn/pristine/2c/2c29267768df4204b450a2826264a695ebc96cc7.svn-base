import importlib
import inspect
import os

output = r"E:\SRC\SYNC_PYTHON\runner"


def create_install(name, method_name, create_task_bat_file, minute, hour=None):
    _name = name.split(".")[-1]
    _path = name.split(".")[0] + "." + name.split(".")[1]
    print(_path, _name, method_name)
    batch_file_name = _name + "_" + method_name + ".vbs"
    batch_file_path = os.path.join(output, batch_file_name)

    if minute == 0 and hour != 0:
        content = """schtasks /create /tn sync_{_path}_{method_name} /tr {batch_file_path} /sc hourly /mo {hour}""".format(hour=hour, _path=_path, method_name=method_name, batch_file_path=batch_file_path)
        create_task_bat_file.write(content + "\n")
    elif hour == 0 and minute !=0:
        content = """schtasks /create /tn sync_{_path}_{method_name} /tr {batch_file_path} /sc minute /mo {minute}""".format(minute=minute, _path=_path, method_name=method_name, batch_file_path=batch_file_path)
        create_task_bat_file.write(content + "\n")
    else:
        pass


def create_delete(name, method_name, delete_task_bat_file, minute, hour=None):
    _name = name.split(".")[-1]
    _path = name.split(".")[0] + "." + name.split(".")[1]
    print(_path, _name, method_name)
    batch_file_name = _name + "_" + method_name + ".bat"
    batch_file_path = os.path.join(output, batch_file_name)
    content = """schtasks /delete /tn "sync_{_path}_{method_name}" /f""".format(minute=minute, _path=_path, method_name=method_name, batch_file_path=batch_file_path)
    delete_task_bat_file.write(content + "\n")



def create_bat_python(name, method_name, minute, hour=None, create_task_bat=None, delete_task_bat=None):
    if not method_name.startswith("_"):
        _name = name.split(".")[-1]
        _path = name.split(".")[0] + "." + name.split(".")[1]
        print(_path, _name, method_name)

        batch_file_name = _name + "_" + method_name + ".bat"
        batch_file_path = os.path.join(output, batch_file_name)
        if os.path.exists(batch_file_path):
            os.remove(batch_file_path)

        vbs_file_name = _name + "_" + method_name + ".vbs"
        vbs_file_path = os.path.join(output, vbs_file_name)
        if os.path.exists(vbs_file_path):
            os.remove(vbs_file_path)

        python_file_name = _name + "_" + method_name + ".py"
        python_file_path = os.path.join(output, python_file_name)
        if os.path.exists(python_file_path):
            os.remove(python_file_path)

        with open(batch_file_path, 'w') as file:
            content = """setlocal enabledelayedexpansion
cd /d %~d0
cd /d %~dp0
set "filename=%~nx0"
set "name=!filename:.bat=!"
set pythonfile=%name%.py

start /B python %pythonfile%
            """
            file.write(content + "\n")

        with open(vbs_file_path, 'w') as file:
            content = """Set objShell = CreateObject("WScript.Shell")
objShell.Run "cmd.exe /c {batch_file_path}", 0, True
                """.format(batch_file_path=batch_file_path)
            file.write(content + "\n")

        with open(python_file_path, 'w', encoding="utf-8") as file:
            content = """import os
import sys
sys.path.insert(0, sys.path[0])
sys.path.insert(0, os.path.join(sys.path[0], ".."))
from xinxiang import main, config
from xinxiang.util import my_log
from {_path} import {_name}

if __name__ == '__main__':
    my_log.init_log(os.path.join(config.g_log_path, "NextChipETL.APS_SYNC.log"))  # 初期化Logger
    {_name}.{method_name}()
                    """.format(_path=_path, _name=_name, method_name=method_name)
            file.write(content + "\n")

        create_install(name, method_name, create_task_bat_file=create_task_bat, minute=minute, hour=hour)
        create_delete(name, method_name, delete_task_bat_file=delete_task_bat, minute=minute, hour=hour)


def get_all_method(module):
    methods = []
    for name, obj in inspect.getmembers(module):
        if inspect.isfunction(obj):
            methods.append(name)
    return methods


if __name__ == '__main__':
    create_task_bat = os.path.join(output, "00_install_task_in_windows.bat")
    if os.path.exists(create_task_bat):
        os.remove(create_task_bat)
    create_task_bat = open(create_task_bat, "w", encoding="utf-8")
    delete_task_bat = os.path.join(output, "00_delete_task_in_windows.bat")
    if os.path.exists(delete_task_bat):
        os.remove(delete_task_bat)
    delete_task_bat = open(delete_task_bat, "w", encoding="utf-8")

    every_minute = 0
    every_hour = 6

    module_name = "xinxiang.jobs_manager.manager_jobs"
    module = importlib.import_module(module_name)
    methods = get_all_method(module)
    for method in methods:
        create_bat_python(module_name, method, minute=every_minute, hour=every_hour, create_task_bat=create_task_bat, delete_task_bat=delete_task_bat)

    every_minute = 5
    every_hour = 0
    module_name = "xinxiang.jobs_sync_his.sync_his_jobs"
    module = importlib.import_module(module_name)
    methods = get_all_method(module)
    for method in methods:
        create_bat_python(module_name, method, minute=every_minute, hour=every_hour, create_task_bat=create_task_bat, delete_task_bat=delete_task_bat)

    _index = 0
    every_minute = 10
    every_hour = 0
    module_name = "xinxiang.jobs_sync_view.sync_view_jobs"
    module = importlib.import_module(module_name)
    methods = get_all_method(module)
    for method in methods:
        _index = _index + 1
        create_bat_python(module_name, method, minute=every_minute, hour=every_hour, create_task_bat=create_task_bat, delete_task_bat=delete_task_bat)
        if 45 <= _index and _index <= 90:
            every_minute = 8
            every_hour = 0
        if _index > 90:
            every_minute = 5
            every_hour = 0

    every_minute = 5
    every_hour = 0
    module_name = "xinxiang.jobs_manager.sync_to_pg_jobs"
    module = importlib.import_module(module_name)
    methods = get_all_method(module)
    for method in methods:
        create_bat_python(module_name, method, minute=every_minute, hour=every_hour, create_task_bat=create_task_bat,
                          delete_task_bat=delete_task_bat)

    create_task_bat.close()
    delete_task_bat.close()