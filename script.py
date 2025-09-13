import boto3
import time

# Configurations - Update as per your environment
SOURCE_ENDPOINT_ARN = 'arn:aws:dms:region:account-id:endpoint:source-endpoint-id'
TARGET_ENDPOINT_ARN = 'arn:aws:dms:region:account-id:endpoint:target-endpoint-id'
REPLICATION_INSTANCE_ARN = 'arn:aws:dms:region:account-id:rep:replication-instance-id'
REPLICATION_TASK_ID = 'rds-migration-task'
SOURCE_DB_ENGINE = 'mysql'  # or 'postgres'
TARGET_DB_ENGINE = 'mysql'  # or 'postgres'
MIGRATION_TABLES = 'schema-name.%'  # Schema and table selection - adjust or use 'full-load'
AWS_REGION = 'your-aws-region'

# Create DMS client
dms_client = boto3.client('dms', region_name=AWS_REGION)

AWS_REGION = 'us-east-1'  # <-- Replace with your AWS region

dms_client = boto3.client('dms', region_name=AWS_REGION)

# Now you can call DMS APIs, e.g.:
response = dms_client.describe_replication_tasks()
print(response)


def create_replication_task():
    try:
        response = dms_client.create_replication_task(
            ReplicationTaskIdentifier=REPLICATION_TASK_ID,
            SourceEndpointArn=SOURCE_ENDPOINT_ARN,
            TargetEndpointArn=TARGET_ENDPOINT_ARN,
            ReplicationInstanceArn=REPLICATION_INSTANCE_ARN,
            MigrationType='cdc',  # change to 'full-load-and-cdc' for live sync with minimal downtime
            TableMappings=f'''
            {{
                "rules": [
                    {{
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "1",
                        "object-locator": {{
                            "schema-name": "%",
                            "table-name": "%"
                        }},
                        "rule-action": "include",
                        "filters": []
                    }}
                ]
            }}
            ''',
            CdcStartPosition='now'  # or specify date/time for CDC start position
        )
        print("Replication task created:", response['ReplicationTask']['ReplicationTaskIdentifier'])
        return response['ReplicationTask']['ReplicationTaskArn']
    except dms_client.exceptions.ResourceAlreadyExistsFault:
        print(f"Replication task {REPLICATION_TASK_ID} already exists.")
        tasks = dms_client.describe_replication_tasks(
            Filters=[{'Name': 'replication-task-id', 'Values': [REPLICATION_TASK_ID]}]
        )
        return tasks['ReplicationTasks'][0]['ReplicationTaskArn']

def start_replication_task(replication_task_arn):
    dms_client.start_replication_task(
        ReplicationTaskArn=replication_task_arn,
        StartReplicationTaskType='start-replication'
    )
    print("Replication task started")

def wait_for_task_ready(replication_task_arn):
    print("Waiting for replication task to be in 'running' status...")
    while True:
        resp = dms_client.describe_replication_tasks(
            Filters=[{'Name': 'replication-task-arn', 'Values': [replication_task_arn]}]
        )
        status = resp['ReplicationTasks'][0]['Status']
        print(f"Current status: {status}")
        if status == 'running':
            break
        elif status in ['failed', 'stopped']:
            raise Exception("Replication task failed or stopped.")
        time.sleep(10)

def stop_replication_task(replication_task_arn):
    dms_client.stop_replication_task(ReplicationTaskArn=replication_task_arn)
    print("Replication task stopped")

def verify_data_consistency():
    # This depends on the DB engine and you might want to run queries or
    # check checksums externally here. For brevity, not implemented.
    print("Verify the data consistency manually or with automated checks")

def migrate_rds():
    task_arn = create_replication_task()
    start_replication_task(task_arn)
    wait_for_task_ready(task_arn)
    
    print("Migration in progress with CDC...")
    print("Perform cutover during a low usage window:")
    
    # Stop writes on source DB before cutover (outside scope of code)
    input("Press Enter to initiate cutover (stop writes on source DB first)...")
    
    stop_replication_task(task_arn)
    
    verify_data_consistency()

    # Change your application connection string to target RDS endpoint here (outside scope)
    print("Application should now redirect traffic to target RDS instance")

    # Optionally delete the replication task or keep for rollback
    # dms_client.delete_replication_task(ReplicationTaskArn=task_arn)

if __name__ == "__main__":
    migrate_rds()
