from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from question_4_a import etl_web_to_gcs

github_block = GitHub.load("zoomcamp-github")

github_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name='github-flow',
    entrypoint='week_2_workflow_orchestration/hw2/question_4_a.py:etl_web_to_gcs',
    storage=github_block
)

if __name__=="__main__":
    github_dep.apply()