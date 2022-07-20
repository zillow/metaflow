from metaflow.plugins.argo.argo_client import ArgoClient

def run_inner_flow(
    flow_name: str, 
    parameters: dict=None, 
    wait=True
):
    if parameters is None:
        parameters = {}
        
    template_name = flow_name.lower()
    
    argo_client_dummy = ArgoClient("aip-example-sandbox")
    
    return argo_client_dummy.trigger_workflow_template(
        template_name, 
        parameters=parameters
    )
    