#!/bin/bash

# Load pyenv environment
export PYENV_ROOT="$HOME/pyenv"
export PYENV_ADSB="$PYENV_ROOT/adsb_rx/bin/activate"
export PATH="$PYENV_ROOT/adsb_rx/bin:$PATH"
export PY_PATH="$HOME/apps/adsb_rx/"
export PY_SRC="get_adsb_data.py"

source $PYENV_ADSB
# Navigate to your project directory
cd $PY_PATH

# Run your Python script with the pyenv-managed Python
python $PY_SRC --silent
# Optional: Deactivate the environment (good practice for clean scripts)
deactivate
