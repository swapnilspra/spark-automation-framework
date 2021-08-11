import pulumi
from pulumi_aws import glue
from typing import List

class WorkflowSchedule(pulumi.ComponentResource):
    workflow:glue.Workflow

    def __init__(self, name,
            source_jobs:List[glue.Job]=None,
            schedule_expr:str=None,
            enabled:bool=False,
            opts=None):
        super().__init__('hca:WorkflowSchedule', name, None, opts)
        
        self.workflow = glue.Workflow(f"{name}-workflow",
            description=f"source job workflow for => {name}",
            opts=pulumi.ResourceOptions(parent=self))

        start_task = glue.Trigger(f"{name}-schedule",
            description=f"run source jobs on schedule for {name}",
            actions=[{ 'jobName': job.name } for job in source_jobs ],
            workflow_name=self.workflow.name,
            schedule=f"cron({schedule_expr})" if enabled else None,
            type='SCHEDULED' if enabled else 'ON_DEMAND',
            opts=pulumi.ResourceOptions(parent=self, delete_before_replace=True))