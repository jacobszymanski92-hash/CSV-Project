@echo off
echo 🚀 GitHub Upload Helper for ETL Pipeline Project
echo.

echo Checking if Git is installed...
git --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Git is not installed or not in PATH
    echo Please install Git from: https://git-scm.com/download/windows
    echo Then restart this script.
    pause
    exit /b 1
)
echo ✅ Git is installed

echo.
echo Initializing Git repository...
git init

echo.
echo Adding all files to Git...
git add .

echo.
echo Creating initial commit...
git commit -m "Initial commit: Complete ETL pipeline with CSV to BigQuery functionality"

echo.
echo ========================================
echo NEXT STEPS:
echo.
echo 1. Create a new repository on GitHub:
echo    https://github.com/new
echo.
echo 2. Repository name: csv-bigquery-etl-pipeline
echo.
echo 3. Description: Complete ETL pipeline for extracting CSV data, cleaning/transforming with Pandas, and loading to Google BigQuery
echo.
echo 4. Choose Public or Private
echo.
echo 5. DO NOT initialize with README (we already have one)
echo.
echo 6. After creating the repository, run these commands:
echo    (Replace YOUR_USERNAME with your GitHub username)
echo.
echo    git remote add origin https://github.com/YOUR_USERNAME/csv-bigquery-etl-pipeline.git
echo    git branch -M main
echo    git push -u origin main
echo.
echo ========================================
echo.
pause