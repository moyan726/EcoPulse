$ErrorActionPreference = "Stop"

# Define the local virtual environment path
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
$VenvPath = Join-Path $ProjectRoot ".venv"
$VenvActivate = Join-Path $VenvPath "Scripts\Activate.ps1"

# Check if .venv exists
if (-not (Test-Path $VenvPath)) {
    Write-Warning "Local virtual environment (.venv) not found!"
    Write-Host "Creating local virtual environment..."
    # Attempt to create it using the known python path (fallback mechanism)
    $BasePython = "E:\Anaconda3\Anaconda_data\python.exe"
    if (Test-Path $BasePython) {
        Start-Process -FilePath $BasePython -ArgumentList "-m venv $VenvPath" -Wait -NoNewWindow
        Write-Host "Virtual environment created successfully."
    } else {
        throw "Cannot find base Python to create venv. Please run 'python -m venv .venv' manually."
    }
}

# Activate the virtual environment
if (Test-Path $VenvActivate) {
    # Dot-sourcing the activation script to run it in the current scope
    # Note: Set-StrictMode is avoided here as it conflicts with standard venv Activate.ps1
    . $VenvActivate
} else {
    throw "Activation script not found at $VenvActivate"
}

# Set Environment Variables
$env:JAVA_HOME = "E:\Java\jdk1.8.0_291"

# Set PySpark Python to the venv's python
$VenvPython = Join-Path $VenvPath "Scripts\python.exe"
$env:PYSPARK_PYTHON = $VenvPython
$env:PYSPARK_DRIVER_PYTHON = $VenvPython

Write-Host "==================================================" -ForegroundColor Green
Write-Host "   Environment Activated: .venv (Local)" -ForegroundColor Green
Write-Host "=================================================="
Write-Host "JAVA_HOME       : $env:JAVA_HOME"
Write-Host "PYSPARK_PYTHON  : $env:PYSPARK_PYTHON"
Write-Host "=================================================="
