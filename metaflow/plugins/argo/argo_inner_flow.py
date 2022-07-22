# from metaflow import Run, namespace
from metaflow.plugins.argo.argo_client import ArgoClient

import time

def run_inner_flow(
    flow_name: str, 
    parameters: dict=None, 
    wait=True
):
    print(f"Running inner flow: {flow_name}")
    
    from metaflow import Run, namespace     # consider cleaning this up so that import is on top
                                            # current issue is that import on top creates circular error
    
    if parameters is None:
        parameters = {}
        
    template_name = flow_name.lower()       # we may have to create argo template here (if it doesn't exist already
                                            # b/c MF flow may exist, but possibly not argo template?
    
    argo_client_dummy = ArgoClient("aip-example-sandbox")
    
    flow_information = argo_client_dummy.trigger_workflow_template(
        template_name, 
        parameters=parameters
    )
    
    inner_run_argo_id = flow_information['metadata']['name']
    inner_run_metaflow_id = "argo-" + inner_run_argo_id
    inner_namespace = flow_information['metadata']['namespace']
    finished = False
    successful = None
    
    output = {
        "inner_run_argo_id": inner_run_argo_id,
        "inner_run_metaflow_id": inner_run_metaflow_id,
        "inner_namespace": inner_namespace,
        "finished": finished,
        "successful": successful,
        "exception": None
    }
    
    if wait:
        print("Waiting for inner flow to run")
        
        namespace(None)
        inner_run_metaflow_location = flow_name + "/" + inner_run_metaflow_id

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
            return output
            
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
                            output["finished"] = True
                            output["successful"] = False
                            output["exception"] = {"step": step, "task": task, "exception": task.exception}
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
                            return output
                    
                # sleep and decrement attempts remaining
                time.sleep(5)
                attempts_to_finish_run -= 1
        
        
        
        print("Steps (final):")
        for step in inner_run.steps():    # what are tags???
            step_str = str(step)
            print(f"step_str: {step_str}")
            print(f"finished at: {step.finished_at}")
            if step_str not in steps:
                print("adding this ^^^ step")
                steps.append(step_str)
            for task in step.tasks():
                print(f"task: {task}")
                if task.exception is not None:
                    print(f"EXCEPTION: {task.exception}")
        print("")
        
        print(f"steps: {steps}")
        
        output["finished"] = inner_run.finished
        output["successful"] = inner_run.successful
        print("Inner flow is finished!!!")
    
    else:
        print("Not waiting for inner flow to finish")
    
    return output
    