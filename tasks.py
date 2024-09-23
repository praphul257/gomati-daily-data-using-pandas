
import os
from celeryconfig import app

@app.task
def run_main():
    # This will run your Python script (main.py)
    os.system('python main.py')
