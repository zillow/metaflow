from metaflow.plugins.argo.argo_client import ArgoClient

def run_inner_flow(template_name):
    argo_client_dummy = ArgoClient("aip-example-sandbox")
    return argo_client_dummy.trigger_workflow_template(template_name)  # add params later
    