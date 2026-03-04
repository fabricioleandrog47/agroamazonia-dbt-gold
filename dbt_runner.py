import os
import subprocess
import sys

cliente_id = os.getenv("CLIENTE_ID", "juca")

print(f"Cliente ID: {cliente_id}")

os.chdir("/dbt")

cmd = [
    "dbt", "run",
    "--profiles-dir", "/opt/dbt",
    "--vars", f"cliente_id: {cliente_id}"
]

print("Running:", " ".join(cmd))

result = subprocess.run(cmd)
sys.exit(result.returncode)
