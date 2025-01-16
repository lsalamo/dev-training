#!/bin/bash
# Make the script executable: chmod +x /Users/luis.salamo/Documents/github/dev-training/.vscode/tasks/shell_script/run_benchmark_tasks.sh

# Define the workspace folder and Python interpreter path
WORKSPACE_FOLDER="/Users/luis.salamo/Documents/github/dev-training"
PYTHON_INTERPRETER="$WORKSPACE_FOLDER/.venv/bin/python"

# Run Benchmark Google
$PYTHON_INTERPRETER $WORKSPACE_FOLDER/src/benchmark_ga_adobe/benchmark_ga.py "$1" "$2" "$3" "$4"

# Run Benchmark Adobe
$PYTHON_INTERPRETER $WORKSPACE_FOLDER/src/benchmark_ga_adobe/benchmark_adobe.py "$1" "$2" "$3" "$4"

# Run Benchmark
$PYTHON_INTERPRETER $WORKSPACE_FOLDER/src/benchmark_ga_adobe/benchmark.py "$1" "$2" "$3" "$4"