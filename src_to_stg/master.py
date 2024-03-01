import concurrent.futures
import subprocess

def call_python_script(script_path):
    try:
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error calling {script_path}: {e}")

scripts_to_call = ['customers.py','products.py','employees.py','offices.py','orderdetails.py','orders.py','payments.py','productlines.py']


def run_script(script):
    call_python_script(script)

if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(run_script, scripts_to_call)
