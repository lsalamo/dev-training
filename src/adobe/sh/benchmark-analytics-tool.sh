# > chmod 0777 "~/Documents/github\ enterprise/python-training/adobe/benchmark-analytics-tool/benchmark-analytics-tool-v2.py"
# ~/Documents/github\ enterprise/python-training/adobe/sh/benchmark-analytics-tool.sh cnet

#!/bin/bash

echo "=====================================  BEGIN EXECUTION ====================================="
echo "Which is the site (cnet | mnet | ma | fc | ij | ijit | hab)?"
read SITE
echo "Which is date from (YYYY-MM-DD)?"
read DATE_FROM
echo "Which is date to (YYYY-MM-DD)?"
read DATE_TO

# ACTIVAMOS ENTORNO
source ~/Applications/anaconda3/etc/profile.d/conda.sh
conda activate python_38_env
echo "INFO > conda environment 'python_38_env' activated"

# RUN PYTHON
FILE=~/Documents/github\ enterprise/python-training/adobe/benchmark-analytics-tool/benchmark-analytics-tool-v2.py
if test -f "$FILE"; then
	echo "INFO > file exists $FILE"
	python3.8 "$FILE" $SITE $DATE_FROM $DATE_TO
else
	echo "ERROR > file does not exist > $FILE"
fi

FILE=~/Documents/github\ enterprise/python-training/adobe/benchmark-analytics-tool/benchmark-analytics-tool-event-v2.py
if test -f "$FILE"; then
	echo "INFO > file exists $FILE"
	python3.8 "$FILE" $SITE $DATE_FROM $DATE_TO
else
	echo "ERROR > file does not exist > $FILE"
fi

echo "=====================================  END EXECUTION   ====================================="