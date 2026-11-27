# Запуск сервісу Harvester у Windows (режим обслуговування).
#
# Робить три речі:
#   1. Піднімає контейнер PostgreSQL `harvester-pg`, якщо він зупинений,
#      і чекає, поки він прийматиме з'єднання на 127.0.0.1:5432.
#   2. Запускає `harvester start` з конфігом проєкту.
#   3. Консольний вивід дублює у logs/service_console.log (основний лог
#      сервіс пише сам у logs/harvester.log згідно з config.yaml).
#
# Запуск: powershell -ExecutionPolicy Bypass -File scripts\start_windows.ps1
# Або просто двічі клікніть start_harvester.cmd у корені проєкту.

$ErrorActionPreference = "Stop"
# UTF-8 для консолі: без цього rich падає на символах типу ✓ (cp1251)
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

$logDir = Join-Path $root "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$consoleLog = Join-Path $logDir "service_console.log"

# --- 1. PostgreSQL у Docker ---------------------------------------------
$pg = docker ps -a --filter "name=^harvester-pg$" --format "{{.Names}} {{.Status}}"
if (-not $pg) {
    Write-Host "Контейнер harvester-pg не знайдено — створюю..."
    docker run -d --name harvester-pg --restart unless-stopped `
        -e POSTGRES_USER=harvester -e POSTGRES_PASSWORD=app5552 `
        -e POSTGRES_DB=harvester -p 127.0.0.1:5432:5432 `
        -v harvester-pgdata:/var/lib/postgresql/data `
        postgres:17 | Out-Null
} elseif ($pg -notmatch "Up") {
    Write-Host "Запускаю контейнер harvester-pg..."
    docker start harvester-pg | Out-Null
}

$deadline = (Get-Date).AddSeconds(90)
while ((Get-Date) -lt $deadline) {
    try {
        $c = New-Object Net.Sockets.TcpClient
        $c.Connect("127.0.0.1", 5432)
        $c.Close()
        break
    } catch {
        Start-Sleep -Seconds 2
    }
}

# --- 2. Сервіс ------------------------------------------------------------
$harvester = Join-Path $root ".venv\Scripts\harvester.exe"
if (-not (Test-Path $harvester)) {
    Write-Error ".venv не знайдено. Створіть його: uv venv --python 3.12 .venv && uv pip install --python .venv -e `".[dev]`""
    exit 1
}

Write-Host "Запуск harvester start (лог: $consoleLog)"
& $harvester start --config (Join-Path $root "config.yaml") *>> $consoleLog
