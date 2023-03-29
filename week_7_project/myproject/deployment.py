from my_project.flows import my_flow
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=my_flow,
    name="example-deployment", 
    version=1, 
    work_queue_name="demo",
)
deployment.apply()