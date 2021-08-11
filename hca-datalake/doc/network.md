# Network Topology

### Description

This file will describe all network ports including source and target.
All component are located in the ohio region.

### Ports table

| Category | source | target | port | comments
| ------ | ----------- |----------- |----------- |----------- |
| Monitoring   | Lambda   | ElasticSearch   |9200   |  When adding Elasticsearch as a subscriber to AWS CloudWatch it will create a lambda (which should be able to send messages to the Elasticsearch service|
| Monitoring   |ElasticSearch   |Kibana   |-   | Kibana is implemented within the Elasticsearch service |
| Monitoring | Workspaces/HCA operational | Kibana | 5601 | To review dashboard and explore logs |
| Alerting | Lambda | outlook.microsoft.com | 443 | Lambda will send "cards" to Microsoft Teams |
| Alerting | SNS | Lambda | -| SNS is sending notifications to subscribers|
| Alerting | Cloudwatch | SNS | -| Cloudwatch is allowed to send notifications to the SNS topic|
| Database | Clients/Apps | Database | 5432 | Postgres port to interact with the database| 
| ETL | Glue | Database | 5432 | Postgres port to interact with the database| 

### Dictionary table
| Name | endpoint/IP |
| ------ | ----------- |
| Monitoring Lambda | LogsToElasticsearch_hca-dl-logs |
| Alerting Lambda | hca-dl-teams-alert-lambda |
| ElasticSearch | vpc-hca-dl-logs-rxe7k6mhsyaifaqf2dzx2z5zny.us-east-2.es.amazonaws.com |
| Kibana | vpc-hca-dl-logs-rxe7k6mhsyaifaqf2dzx2z5zny.us-east-2.es.amazonaws.com/_plugin/kibana |
| Workspaces | * |
| SNS | hca-dl-updates |
| Database | hca-dl-data.c4upoxiov3wc.us-east-2.rds.amazonaws.com |
| Glue | all jobs in the ohio region |
