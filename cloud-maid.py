import schedule
import time
import threading
import queue
import yaml
import subprocess
import json
import os
import signal
import logging
import concurrent.futures
from functools import partial
from google.cloud import pubsub_v1

# Configure logging
logging.basicConfig(level=logging.INFO, format='MAID | %(levelname)s | %(message)s')

# Print Header
def print_header(file_path):
	with open(file_path, "r", encoding="utf8") as maidchan:
		maid_chan = maidchan.readlines()
	for x in maid_chan:
		print(x.replace("\n",""))

# Ignore Sending Exceptions down to subprocess
def preexec_function():
	signal.signal(signal.SIGINT, signal.SIG_IGN)

# Job Running Function
def job(script, script_params):
	script_path = script + ".py"
	logging.info(f"Executing job: {script} on project: {script_params['project_id']}")
	try:
		subprocess.run(
			[
				"python3",
				script_path,
				json.dumps(script_params).replace(" ","")
			],
			preexec_fn=preexec_function)
		logging.info(f"Completed job: {script} on project: {script_params['project_id']}")
	except subprocess.CalledProcessError as e:
		logging.error(f"{script} on project: {script_params['project_id']} failed: {e}")
	time.sleep(2)

# Serving PubSub
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
	logging.info(f"Order Received | {message.data.decode("utf-8")}.")
	if message.attributes:
		must_have = ["project_id", "contact", "sa", "module_id"]
		if all(must in message.attributes for must in must_have):
			logging.info(f"Order Contains Correct Values.")
			order_params = {
				"project_id": message.attributes.pop("project_id"),
				"runtime": "Order",
				"contact": message.attributes.pop("contact"),
				"sa": message.attributes.pop("sa"),
				"extra": {key: message.attributes.get(key) for key in message.attributes}
			}
			order_module = message.attributes.pop("module_id")
			logging.info(f"Putting to Queue: {order_module}.")
			jobqueue.put(partial(job, order_module, order_params))
		else:
			logging.error(f"Order attributes are incorrect.")
	else:
		logging.error(f"Order is missing attributes.")
	message.ack()

# Subcription Listener Thread
def order_worker(orders, subscription):
	if orders and subscription:
		os.environ["GRPC_VERBOSITY"] = "ERROR"
		subscriber = pubsub_v1.SubscriberClient()
		streaming_pull_future = subscriber.subscribe(subscription, callback=callback)
		logging.info(f"Listening for orders on: {subscription}.")
		if TERMINATE :
			logging.info(f"Stopping Orders Subscription.")
			streaming_pull_future.cancel()
			streaming_pull_future.result()
	else:
		logging.info(f"Maid is not listening for orders.")

# Main Thread Worker
def worker_main():
	tasks_count = 0
	with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
		while True:
			if not jobqueue.empty():
				job_func = jobqueue.get(block=False)
				executor.submit(job_func)
				jobqueue.task_done()
			if TERMINATE :
				if executor._work_queue.qsize() > 1 :
					if tasks_count != executor._work_queue.qsize() :
						logging.info(f"Maid awaiting remaining tasks: {executor._work_queue.qsize()}")
						tasks_count = executor._work_queue.qsize()
					time.sleep(2)
				else:
					logging.info(f"Maid is finishing last tasks.")
					executor.shutdown()
					break

def load_config(file_path):
	with open('config.yaml') as file:
		return yaml.load(file, Loader=yaml.FullLoader)

def list_jobs():
	jobs = schedule.get_jobs()
	logging.info("Listing Jobs:")
	for counter, room in enumerate(jobs, start=1):
			logging.info(f"{counter}: {room.job_func.args[0].args[0]} : {room.job_func.args[0].args[1]}")

def main():
	# Initializations
	global TERMINATE, jobqueue, subscription
	TERMINATE = False
	ORDERS = False
	print_header("maid-chan.txt")
	config = load_config('config.yaml')
	default_runtime = config["utc-runtime"]
	if "subscription" in config:
		subscription = config["subscription"]
	else:
		subscription = None
	jobqueue = queue.Queue(maxsize=10)

	# Generate jobs schedule
	for project_id, project in config["projects"].items():
		contact = project["contact"]
		impersonate_service_account = project["impersonate-service-account"]
		if "utc-runtime" in project:
			runtime = project["utc-runtime"]
		else:
			runtime = default_runtime
		for module in project["modules"]:
			module_id = next(iter(module))
			module_data = module[module_id]
			module_runtime = module_data.get("utc-runtime", runtime)
			module_contact = module_data.get("contact", contact)
			module_sa = module_data.get("impersonate-service-account", impersonate_service_account)
			extra = module.get("extra")
			params = {
				"project_id":	project_id,
				"runtime" : module_runtime,
				"contact" : module_contact,
				"sa" : module_sa,
			}
			if extra:
				params["extra"]=json.dumps(extra)

			schedule.every().day.at(module_runtime).do(jobqueue.put, partial(job, module_id, params))

	list_jobs()

	# Setting Up Main Threads
	worker_thread = threading.Thread(target=worker_main, name="Main Thread")
	worker_thread.start()
	order_thread = threading.Thread(target=order_worker, name="Order Thread", args = [ORDERS, subscription])
	order_thread.start()

	# Run Loop
	try :
		while 1:
			schedule.run_pending()
			time.sleep(1)
	except KeyboardInterrupt:
		print("\r", end="")
		logging.info(f"Exception Detected.")
		logging.info(f"Clearing Schedule.")
		schedule.clear()
		logging.info(f"Setting Termination Flag and awaiting all process to stop")
		TERMINATE = True
		worker_thread.join()
		order_thread.join()
		logging.info(f"Maid Gracefully Exited")

if __name__ == "__main__":
	main()