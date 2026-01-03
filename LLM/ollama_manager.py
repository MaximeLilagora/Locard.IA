import subprocess
import os
import time

# Global variable to store the working process
current_process = None

def start_ollama(parallel_count):
    global current_process
    env = os.environ.copy()

    
    # 1. Checking if a server is already running. If so, kill the process.
    if current_process is not None:
        print(f"🔄 Reloading : Stopping current server...")
        current_process.terminate()
        current_process.wait()                                  # Waiting to free the port
        time.sleep(1)                                           # Small pause for security
    
    # 2. Preparing variables
    env_vars = os.environ.copy()
    env_vars["LLAMA_NUM_PARALLEL"] = str(parallel_count)        # Nb of parallel loader. Do not exceed 8 on Apple Silicon, otherwise you will reach the RAM bandwidth bottleneck
    env_vars["OLLAMA_MAX_LOADED_MODELS"] = "1"                  # We keep only one type of Model working
    
    # 3. Starting server
    print(f"🚀 Warming Ollama with {parallel_count} workers")
    current_process = subprocess.Popen(
        ["ollama", "serve"],
        env=env_vars,
        stdout=subprocess.DEVNULL,                              # Does not show the Ollama continous log
        stderr=subprocess.DEVNULL
    )
    print("✅ Server ready.")

