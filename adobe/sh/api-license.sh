# > chmod 0777 ~/Documents/github/python-training/adobe/api-license/api-license.py
# ~/Documents/github/python-training/adobe/sh/api-license.sh

#!/bin/bash
echo "=====================================  BEGIN EXECUTION ====================================="
echo "Which is date from (YYYY-MM-DD)?"
read DATE_FROM
echo "Which is date to (YYYY-MM-DD)?"
read DATE_TO

# ACTIVAMOS ENTORNO
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate python_38_env
echo "INFO > conda environment 'python_38_env' activated"

# RUN PYTHON
FILE=~/Documents/github/python-training/adobe/api-license/api-license.py
if test -f "$FILE"; then
	python3.8 "$FILE" $DATE_FROM $DATE_TO
else
	echo "ERROR > file does not exist > $FILE"
fi

echo "=====================================  END EXECUTION   ====================================="