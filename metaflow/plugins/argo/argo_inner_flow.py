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
    data = None
    
    output = {
        "inner_run_argo_id": inner_run_argo_id,
        "inner_run_metaflow_id": inner_run_metaflow_id,
        "inner_namespace": inner_namespace,
        "finished": finished,
        "successful": successful,
        "data": data,
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
            
        print(f"Inner flow has started: {inner_run}")
        
        print("Steps (starting):")
        steps = set()
        for step in inner_run.steps():    # what are tags???
            print(step.)
            if step not in steps:
                steps.add(step)
        
        
        print(f"steps: {steps}")
        
        ### CURRENT ISSUE IS I NEED TO CONVERT STEP TO A string SO I CAN KEEP TRACK OF IN SET. SEE DOCUMENTATION TO DIG DOWN.
        ### Also, I should look outside the .client file b/c there may be more stuff in dev files???
        

        attempts_to_finish_run = 30  # in minutes (5s for testing)
        while attempts_to_finish_run > 0:
            if inner_run.finished:
                break
            else:
                if attempts_to_finish_run % 12 == 5:
                    print("Inner flow still running, check back in one minute (5s for testing)")
                time.sleep(5)
                attempts_to_finish_run -= 1
        
        
        
        print("Steps (final):")
        for step in inner_run.steps():    # what are tags???
            print(step)
            if step not in steps:
                print("adding this ^^^ step")
                steps.add(step)
        
        print(f"steps: {steps}")
        
        output["finished"] = inner_run.finished
        output["successful"] = inner_run.successful
        output["data"] = inner_run.data
        print("Inner flow is finished!!!")
    
    else:
        print("Not waiting for inner flow to finish")
    
    return output
    