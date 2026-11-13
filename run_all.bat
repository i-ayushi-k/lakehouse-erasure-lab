@echo off
REM Reproduce everything on Windows CMD: venv -> tests -> demo -> benchmark -> notebook.
cd /d "%~dp0"

python -m venv .venv || exit /b 1
call .venv\Scripts\activate.bat
python -m pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt || exit /b 1

set PYTHONWARNINGS=ignore
python -m pytest -q || exit /b 1
python -m src.erasure.pipeline > results\demo_output.txt || exit /b 1
python scripts\benchmark.py || exit /b 1
python scripts\create_notebook.py || exit /b 1
python -m jupyter nbconvert --to notebook --execute --inplace notebooks\results.ipynb || exit /b 1
echo Done. See results\ and notebooks\results.ipynb
