# from metaflow import Run, namespace
from metaflow.plugins.argo.argo_client import ArgoClient
import time

class TriggeredRun():
    
    def __init__(
        self, 
        flow_name: str, 
        parameters: dict = None, 
        wait: bool = True,
    ):
        
        self._cool_count = 0
        print(f"Starting run of flow: {flow_name}")
    
        from metaflow import Run, namespace     # consider cleaning this up so that import is on top
                                                # current issue is that import on top creates circular error
        
        if parameters is None:
            parameters = {}

        self._flow_name = flow_name
        self._template_name = self._flow_name.lower()
        self._parameters = parameters
        self._wait = wait
        
        self._argo_client = ArgoClient("aip-example-sandbox")
        # Should we add a line where argo_client creates a workflow based on the most recent Metaflow workflow???
        self._flow_information = self._argo_client.trigger_workflow_template(
            self._template_name, 
            parameters=self._parameters,
        )

        self._inner_run_argo_id = self._flow_information['metadata']['name']
        self._inner_run_metaflow_id = "argo-" + self._inner_run_argo_id
        self._inner_namespace = self._flow_information['metadata']['namespace']
        self._finished = False
        self._successful = False
        self._exception = None
        self._status = None
    
        if wait:
            print("Waiting for inner flow to run")

            namespace(None)
            inner_run_metaflow_location = self._flow_name + "/" + self._inner_run_metaflow_id

            attempts_to_begin_run = 8  # gives 2 minutes for inner flow to start; not sure if this should be more or less
            while attempts_to_begin_run > 0:
                try:
                    inner_run = Run(inner_run_metaflow_location)
                    break
                except:
                    print("not ready yet - give me 10 more seconds")
                    time.sleep(10)
                    attempts_to_begin_run -= 1

            if attempts_to_begin_run == 0:
                print("Inner flow failed to begin running")
                return

            print(f"Inner flow has started: {inner_run}\nNow we will wait for the flow to finish")

            steps = []
            attempts_to_finish_run = 30  # in minutes (5s for testing)
            while attempts_to_finish_run > 0:
                if inner_run.finished:
                    break
                else:

                    # if attempts_to_finish_run % 12 == 5:
                    print("Inner flow still running, check back in one minute (5s for testing)")

                    for step in inner_run.steps():
                        step_str = str(step)
                        if step_str not in steps:
                            print(f"Adding step: {step_str}")
                            steps.append(step_str)

                        for task in step.tasks():
                            if task.exception is not None:
                                self._finished = True
                                self._successful = False
                                self._exception = {"step": step, "task": task, "exception": task.exception}
                                print(f"EXITING EARLY BC OF FAILURE @ TASK: {task}")
                                print(f"type(task.exception): {type(task.exception)}")
                                print(f"task.finished: {task.finished}")
                                print(f"task.successful: {task.successful}")
                                print("waiting a minute to confirm...")
                                time.sleep(60)
                                print(f"type(task.exception): {type(task.exception)}")
                                print(f"task.finished: {task.finished}")
                                print(f"task.successful: {task.successful}")
                                print("ok, now exiting run flow function")
                                return

                    # sleep and decrement attempts remaining
                    time.sleep(5)
                    attempts_to_finish_run -= 1



            print("Steps (final):")
            for step in inner_run.steps():    # what are tags???
                step_str = str(step)
                print(f"step_str: {step_str}")
                if step_str not in steps:
                    steps.append(step_str)
                for task in step.tasks():
                    if task.exception is not None:
                        print(f"task: {task}")
                        print(f"EXCEPTION: {task.exception}")

            self._finished = inner_run.finished
            self._successful = inner_run.successful
            print("/nInner flow is finished!!!")

        else:
            print("Not waiting for inner flow to finish")

    
    @property
    def cool_count(self):
        self._cool_count += 1
        return self._cool_count
    
    @property
    def status(self):
        
        print(f"inner run id: {self._inner_run_argo_id}\n")
        ac = self._argo_client
        wf = ac.get_workflow(self._inner_run_argo_id)

        # for key in wf:
        #     if key == "status":
        #         print("STATUS:\n")
        #         print(f"conditions: {wf['status']['conditions']}\n")
        #         print(f"phase: {wf['status']['phase']}\n")
        #         print(f"progress: {wf['status']['progress']}\n")
        #         print("KEYS:")
        #         for status_key in wf["status"]:
        #             print(f"key: {status_key}")
        #     else:
        #         print(f"Non-Status Key {key}: {wf[key]}\n")
        if wf['status'] and wf['status']['phase']:
            self._status = wf['status']['phase']
        
        return self._status
    
    
    
    