import subprocess

def run_script(script_name):
    try:
        print(f"Starting execution of {script_name}...")
        # Run the script and wait for it to complete
        subprocess.run(["python", script_name], check=True)
        print(f"Completed execution of {script_name}.\n")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running {script_name}: {e}")
        raise

if __name__ == "__main__":
    # List of scripts to execute
    scripts = ["main.py", "stage_prep.py", "orchestration_data_loading.py"]

    for script in scripts:
        run_script(script)
