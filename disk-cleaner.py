import time
import sys
import json
from datetime import datetime

'''
This is just a dummy simulation of the disk cleaner module.

'''

args = sys.argv
config = json.loads(args[1])

def sPrint(msg):
  for line in msg.split("\n"):
    text = f"{    config["project_id"]} | disk-cleaner | {config["runtime"]} :  {line}"
    print(text)

sPrint(f"Module : Disk Cleaner\nTime   : {datetime.now()}")
try:
  sPrint(f"  project: {config["project_id"]}")
  sPrint(f"  impersonate: {config["sa"]}")
  sPrint(f"  run at: {config["runtime"]}")
  sPrint(f"  contact: {config["contact"]}")
  if "extra" in config:
    sPrint(f"extra: {config["extra"]}")
  sPrint("Listing disks...")
  time.sleep(10)
  sPrint("Disk list complete.")
  sPrint("Cleaning disks...")
  time.sleep(10)
  sPrint("Disk cleaning complete.")  
except Exception as e:
  sPrint(f"An error occurred: {e}")