import pulumi
from pulumi_aws import iam, glue
from typing import Dict, List
import yaml

class WorkflowDefinition(object):
    def __init__(self, filepath): 
        with open(filepath) as f:
            data = yaml.safe_load(f)

        # strip root element
        _, wfconfig = data.popitem()
        self.__dict__.update(wfconfig)

    # get list of jobs with no dependencies
    def root_jobs(self) -> List[str]:
        #print(f"root_jobs => {self.tasks}")
        return [ task['job'] for task_name, task in self.tasks.items() if not 'dependencies' in task]

    # get dict of tasks with dependencies
    def tasks_with_dependencies(self) -> Dict[str,object]:
        return dict(filter(lambda task: 'dependencies' in task[1] , self.tasks.items()))

class JobWorkflow(pulumi.ComponentResource):
    tasks:Dict[str,glue.Trigger]
    workflow:glue.Workflow

    def __init__(self, name,
            wf_definition:WorkflowDefinition=None,
            glue_jobs_map:Dict[str,glue.Job]=None, # from infra.glue_jobs
            tags:Dict[str,str]=None,
            opts=None):
        super().__init__('hca:JobWorkflow', name, None, opts)
        self.tasks = {}

        self.workflow = glue.Workflow(f"{name}-workflow", 
            description=f"workflow for glue etl jobs => {name}",
            opts=pulumi.ResourceOptions(parent=self))

        # create method to fetch actual job name from pretty job name
        def actual_job_name(job_name:str) -> str:
            if not job_name in glue_jobs_map:
                raise Exception(f"job name [{job_name}] not found in in jobs map {glue_jobs_map}")
            return glue_jobs_map[job_name].glue_job.name

        # if scheduled, create root step
        if wf_definition.type == 'SCHEDULED':
            self.tasks['root'] = glue.Trigger(f"{name}_schedule",
                description=wf_definition.description,
                actions=[{ 'jobName': actual_job_name(n) } for n in wf_definition.root_jobs() ],
                workflow_name=self.workflow.name,
                schedule=f"cron({wf_definition.schedule_interval})",
                type='SCHEDULED',
                opts=pulumi.ResourceOptions(parent=self, delete_before_replace=True))
        
        # type == ON_DEMAND
        else:
            self.tasks['root'] = glue.Trigger(f"{name}_start",
                description=wf_definition.description,
                actions=[{ 'jobName': actual_job_name(n) } for n in wf_definition.root_jobs() ],
                workflow_name=self.workflow.name,
                type='ON_DEMAND',
                opts=pulumi.ResourceOptions(parent=self, delete_before_replace=True))

        # create glue trigger for all other tasks (with dependencies)
        for task_name, task in wf_definition.tasks_with_dependencies().items():

            # collect job dependencies for current task
            dependencies = [ actual_job_name(t['job']) for tname, t in wf_definition.tasks.items() if tname in task['dependencies'] ]

            # create trigger for job with job dependencies
            self.tasks[task_name] = glue.Trigger(f"{name}_{task_name}",
                description=f"trigger for {name} -> {task_name}",
                type='CONDITIONAL',
                actions=[{
                    'jobName': actual_job_name(task['job']),
                }],
                predicate={
                    'conditions': [{ 'jobName': j, 'state':'SUCCEEDED'} for j in dependencies],
                },
                workflow_name=self.workflow.name,
                opts=pulumi.ResourceOptions(parent=self))
