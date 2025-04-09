#activate virtual environment
echo activating virtual environment...
source ./.venv/bin/activate
#check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Virtual environment is not activated."
    exit 1
fi
echo "Virtual environment is activated."

#install dependencies
echo installing dependencies...
pip install -r requirements.txt
#check if dependencies are installed
if [ $? -ne 0 ]; then
    echo "Failed to install dependencies."
    exit 1
fi
echo "Dependencies installed successfully."

#run script
echo running main.py...
python ./main.py

#deactivate virtual environment
echo deactivating virtual environment...
deactivate
#check if virtual environment is deactivated
if [ -n "$VIRTUAL_ENV" ]; then
    echo "Virtual environment is still activated."
    exit 1
fi
echo "Virtual environment is deactivated."