# ... "-NoExit -Command Start-Sleep -Seconds 5 ; conda activate ..."
Start-Process powershell -ArgumentList "-NoExit -Command python -m uvicorn main:app --port 8011 --reload"
Start-Process powershell -ArgumentList "-NoExit -Command python -m uvicorn main:app --port 8012 --reload"
Start-Process powershell -ArgumentList "-NoExit -Command python -m uvicorn main:app --port 8013 --reload"
Start-Process powershell -ArgumentList "-NoExit -Command python -m uvicorn main:app --port 8014 --reload"

