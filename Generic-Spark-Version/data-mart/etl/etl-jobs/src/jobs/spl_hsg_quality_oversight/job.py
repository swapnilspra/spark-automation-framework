from typing import Dict, List, Any
import pyspark.sql
import common.utils
import pyspark.sql.functions as F
from common.etl_job import ETLJob # must be imported after spark has been set up
class Job(ETLJob):
    target_table = "spl_hsg_quality_oversight"
    business_key = "id"
    primary_key = "id"
    sources:Dict[str,Dict[str,Any]] = {
        "hsg": 
            {
                "type": "file",
                "source": "hsg_quality_control_oversight"
            },
        "ldap_reviewer":
            {
                "type": "file",
                "source": "ldap_users"
            },
        "ldap_representative":
            {
                "type": "file",
                "source": "ldap_users"
            },
        "ldap_manager":
            {
                "type": "file",
                "source": "ldap_users"
            },
        "ldap_created_by":
            {
                "type": "file",
                "source": "ldap_users"
            },
        "ldap_modified_by":
            {
                "type": "file",
                "source": "ldap_users"
            },
    }
    joins:List[Dict[str,Any]] = [
        {
            "source": "hsg"
        },
        {
            "source": "ldap_reviewer",
            "conditions": [
                F.col("hsg.ReviewerNameId")==F.col("ldap_reviewer.id"),
                F.lower(F.col("ldap_reviewer.contenttype"))=="person",
                ~F.isnull(F.col("ldap_reviewer.sipaddress"))
            ],
            "type":"leftouter"
        },
        {
            "source": "ldap_manager",
            "conditions": [
                F.col("hsg.ManagerNameId")==F.col("ldap_manager.id"),
                F.lower(F.col("ldap_manager.contenttype"))=="person",
                ~F.isnull(F.col("ldap_manager.sipaddress"))
            ],
            "type":"leftouter"
        },
        {
            "source": "ldap_created_by",
            "conditions": [
                F.col("hsg.CreatedById")==F.col("ldap_created_by.id"),
                F.lower(F.col("ldap_created_by.contenttype"))=="person",
                ~F.isnull(F.col("ldap_created_by.sipaddress"))
            ],
            "type":"leftouter"
        },
        {
            "source": "ldap_modified_by",
            "conditions": [
                F.col("hsg.ModifiedById")==F.col("ldap_modified_by.id"),
                F.lower(F.col("ldap_modified_by.contenttype"))=="person",
                ~F.isnull(F.col("ldap_modified_by.sipaddress"))
            ],
            "type":"leftouter"
        },
        {
            "source": "ldap_representative",
            "conditions": [
                F.col("hsg.RepresentativeNameId")==F.col("ldap_representative.id"),
                F.lower(F.col("ldap_representative.contenttype"))=="person",
                ~F.isnull(F.col("ldap_representative.sipaddress"))
            ],
            "type":"leftouter"
        },
    ]
    target_mappings:List[Dict[str,Any]] = [
        { "source": F.col("hsg.id"), "target": 'id' },
        { "source": F.col("hsg.title"), "target": 'title' },
        { "source": F.col("ldap_reviewer.name"), "target": 'reviewer_name' },
        { "source": F.col("hsg.AWDItemReviewDate"), "target": 'awd_item_review_date' },
        { "source": F.col("hsg.WorkType"), "target": 'work_type' },
        { "source": F.col("ldap_representative.name"), "target": 'representative_name' },
        { "source": F.col("hsg.ProcessorQCErValue"), "target": 'processor_qc' },
        { "source": F.col("hsg.AccountNumber"), "target": 'account_number' },
        { "source": F.col("hsg.FundNumber"), "target": 'fund_number' },
        { "source": F.col("hsg.PassFailValue"), "target": 'pass_fail' },
        { "source": F.col("hsg.ReviewerComments"), "target": 'reviewer_comments' },
        { "source": F.col("ldap_manager.name"), "target": 'manager_name' },
        { "source": F.col("ldap_created_by.name"), "target": 'created_by' },
        { "source": F.col("ldap_modified_by.name"), "target": 'modified_by' },
        { "source": F.to_timestamp("hsg.created"), "target": "created" },
        { "source": F.to_timestamp("hsg.modified"), "target": "modified" },
        { "source": F.col("hsg.ContentType"), "target": 'content_type' },
        # { "source": F.col("hsg.type"), "target": 'type' }, # missing
    ]
