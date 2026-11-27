@echo off
rem Запуск сервісу Harvester (двохкліковий ярлик для Windows).
powershell -ExecutionPolicy Bypass -NoProfile -File "%~dp0scripts\start_windows.ps1"
pause
