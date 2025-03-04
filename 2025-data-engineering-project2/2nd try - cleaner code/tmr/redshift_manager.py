import boto3
import configparser
import logging
import argparse
from pathlib import Path
from typing import Optional
from botocore.exceptions import ClientError
import psycopg2
import json
import time 

class RedshiftInfraManager:
    """Manages AWS Redshift infrastructure including IAM roles, security groups, and clusters."""

    def __init__(self, config_file: str = 'cluster.config', region: str = 'us-east-1'):
        self._setup_logging()
        self.config = self._load_config(config_file)
        self._validate_config(self.config)
        self.clients = self._init_aws_clients(region)

    def _setup_logging(self):
        """Configure logging with file handler."""
        logging.basicConfig(
            filename='redshift_infra.log',
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _load_config(self, config_file: str) -> configparser.ConfigParser:
        """Load configuration from file."""
        config = configparser.ConfigParser()
        if not Path(config_file).exists():
            raise FileNotFoundError(f"Config file '{config_file}' not found.")
        config.read(config_file)
        return config

    def _init_aws_clients(self, region: str) -> dict:
        """Initialize AWS service clients."""
        aws_config = {
            'region_name': region,
            'aws_access_key_id': self.config.get('AWS', 'KEY'),
            'aws_secret_access_key': self.config.get('AWS', 'SECRET')
        }
        return {
            'ec2': boto3.client('ec2', **aws_config),
            'iam': boto3.client('iam', **aws_config),
            'redshift': boto3.client('redshift', **aws_config)
        }

    def _handle_client_error(self, resource_type: str, action: str, error: ClientError):
        """Handle client errors with consistent logging."""
        if error.response['Error']['Code'] == 'EntityAlreadyExists':
            self.logger.info(f"{resource_type} already exists during {action}.")
        else:
            self.logger.error(f"Failed to {action} {resource_type}: {str(error)}")

    def _validate_config(self, config: configparser.ConfigParser) -> None:
        """Validate all required configuration settings."""
        required_sections = ['AWS', 'IAM_ROLE', 'SECURITY_GROUP', 'INBOUND_RULE', 'DWH']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")

    def _wait_for_cluster_available(self, timeout_minutes: int = 20) -> bool:
        """Wait for cluster to become available."""
        cluster_id = self.config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
        for _ in range(timeout_minutes * 2):
            try:
                response = self.clients['redshift'].describe_clusters(
                    ClusterIdentifier=cluster_id
                )
                status = response['Clusters'][0]['ClusterStatus']
                if status == 'available':
                    return True
                self.logger.info(f"Cluster status: {status}, waiting...")
                time.sleep(30)
            except ClientError:
                self.logger.info("Waiting for cluster to be created...")
                time.sleep(30)
        return False


    def get_cluster_endpoint(self) -> Optional[str]:
        """Get the endpoint of the created cluster."""
        try:
            response = self.clients['redshift'].describe_clusters(
                ClusterIdentifier=self.config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
            )
            return response['Clusters'][0]['Endpoint']['Address']
        except ClientError as e:
            self.logger.error(f"Failed to get cluster endpoint: {str(e)}")
            return None
    

    def create_iam_role(self) -> Optional[str]:
        """Create IAM role for Redshift with necessary permissions."""
        role_name = self.config.get('IAM_ROLE', 'NAME')
        try:
            role_policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "redshift.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }]
            }
            response = self.clients['iam'].create_role(
                Path='/',
                RoleName=role_name,
                Description=self.config.get('IAM_ROLE', 'DESCRIPTION'),
                AssumeRolePolicyDocument=json.dumps(role_policy)
            )
            self.clients['iam'].attach_role_policy(
                RoleName=role_name,
                PolicyArn=self.config.get('IAM_ROLE', 'POLICY_ARN')
            )
            self.logger.info(f"Successfully created IAM role: {role_name}")
            return response['Role']['Arn']
        except ClientError as e:
            self._handle_client_error("IAM role", "create", e)
            return self.clients['iam'].get_role(RoleName=role_name)['Role']['Arn']

    def create_security_group(self) -> Optional[str]:
        """Create security group for Redshift cluster."""
        group_name = self.config.get('SECURITY_GROUP', 'NAME')

        try:
            existing_groups = self.clients['ec2'].describe_security_groups(
                Filters=[{'Name': 'group-name', 'Values': [group_name]}]
            )['SecurityGroups']
            if existing_groups:
                self.logger.info(f"Security group {group_name} already exists")
                return existing_groups[0]['GroupId']
            vpcs = self.clients['ec2'].describe_vpcs(
                Filters=[{'Name': 'isDefault', 'Values': ['true']}]
            )['Vpcs']
            if not vpcs:
                raise ValueError("No default VPC found.")
            vpc_id = vpcs[0]['VpcId']

            response = self.clients['ec2'].create_security_group(
                GroupName=group_name,
                Description=self.config.get('SECURITY_GROUP', 'DESCRIPTION'),
                VpcId=vpc_id
            )
            group_id = response['GroupId']
            self.clients['ec2'].authorize_security_group_ingress(
                GroupId=group_id,
                IpProtocol=self.config.get('INBOUND_RULE', 'PROTOCOL'),
                FromPort=int(self.config.get('INBOUND_RULE', 'PORT_RANGE')),
                ToPort=int(self.config.get('INBOUND_RULE', 'PORT_RANGE')),
                CidrIp=self.config.get('INBOUND_RULE', 'CIDRIP')
            )
            self.logger.info(f"Successfully created security group: {group_name}")
            return group_id
        except ClientError as e:
            self._handle_client_error("security group", "create", e)
            return None

    def create_redshift_cluster(self, role_arn: str, security_group_id: str) -> bool:
        """Create Redshift cluster with specified configurations."""
        try:
            self.clients['redshift'].create_cluster(
                ClusterIdentifier=self.config.get('DWH', 'DWH_CLUSTER_IDENTIFIER'),
                ClusterType=self.config.get('DWH', 'DWH_CLUSTER_TYPE'),
                NodeType=self.config.get('DWH', 'DWH_NODE_TYPE'),
                NumberOfNodes=int(self.config.get('DWH', 'DWH_NUM_NODES')),
                DBName=self.config.get('DWH', 'DWH_DB'),
                MasterUsername=self.config.get('DWH', 'DWH_DB_USER'),
                MasterUserPassword=self.config.get('DWH', 'DWH_DB_PASSWORD'),
                VpcSecurityGroupIds=[security_group_id],
                IamRoles=[role_arn]
            )
            self.logger.info("Successfully initiated Redshift cluster creation.")
            return True
        except ClientError as e:
            self.logger.error(f"Failed to create Redshift cluster: {str(e)}")
            return False

    def cleanup(self) -> bool:
        success = True

        # Delete Redshift cluster and wait for it to be fully deleted
        try:
            self.clients['redshift'].delete_cluster(
                ClusterIdentifier=self.config.get('DWH', 'DWH_CLUSTER_IDENTIFIER'),
                SkipFinalClusterSnapshot=True
            )
            self.logger.info("Initiated deletion of Redshift cluster.")
            self.wait_for_cluster_deleted(self.config.get('DWH', 'DWH_CLUSTER_IDENTIFIER'))
        except ClientError as e:
            self.logger.error(f"Failed to delete Redshift cluster: {str(e)}")
            success = False

        # Delete security group
        try:
            self.clients['ec2'].delete_security_group(
                GroupName=self.config.get('SECURITY_GROUP', 'NAME')
            )
            self.logger.info("Deleted security group.")
        except ClientError as e:
            self.logger.error(f"Failed to delete security group: {str(e)}")
            success = False

        # Delete IAM role
        try:
            role_name = self.config.get('IAM_ROLE', 'NAME')
            self.clients['iam'].detach_role_policy(
                RoleName=role_name,
                PolicyArn=self.config.get('IAM_ROLE', 'POLICY_ARN')
            )
            self.clients['iam'].delete_role(RoleName=role_name)
            self.logger.info("Deleted IAM role.")
        except ClientError as e:
            self.logger.error(f"Failed to delete IAM role: {str(e)}")
            success = False

        return success

    def wait_for_cluster_deleted(self, cluster_identifier: str, timeout: int = 600) -> bool:
        """Wait for the Redshift cluster to be fully deleted."""
        import time
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                self.clients['redshift'].describe_clusters(ClusterIdentifier=cluster_identifier)
                self.logger.info("Cluster is still being deleted. Waiting...")
                time.sleep(10)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ClusterNotFound':
                    self.logger.info(f"Cluster {cluster_identifier} has been deleted.")
                    return True
                self.logger.error(f"Failed to check cluster deletion status: {str(e)}")
                return False
        self.logger.error("Timeout waiting for cluster deletion.")
        return False
    

    def connect_to_redshift(self):
        """Connect to the Redshift cluster with improved error handling and resource management."""
        try:
            endpoint = self.get_cluster_endpoint()
            if not endpoint:
                self.logger.error("Failed to retrieve cluster endpoint.")
                return

            with psycopg2.connect(
                dbname=self.config.get('DWH', 'DWH_DB'),
                user=self.config.get('DWH', 'DWH_DB_USER'),
                password=self.config.get('DWH', 'DWH_DB_PASSWORD'),
                host=endpoint,
                port=self.config.get('DWH', 'DWH_PORT', fallback='5439'),
                connect_timeout=30
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    result = cursor.fetchone()
                    self.logger.info(f"Redshift version: {result[0]}")
        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to connect to Redshift: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during Redshift connection: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description="AWS Redshift Infrastructure Manager")
    parser.add_argument("--create", action="store_true", help="Create infrastructure")
    parser.add_argument("--delete", action="store_true", help="Delete infrastructure")
    parser.add_argument("--config", default="cluster.config", help="Config file path")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--connect", action="store_true", help="Connect to Redshift cluster")
    args = parser.parse_args()

    print("\n=== AWS Redshift Infrastructure Manager ===")
    manager = RedshiftInfraManager(config_file=args.config, region=args.region)

    if args.create:
        print("\nStarting infrastructure creation...")
        
        print("\n1. Creating IAM Role...")
        role_arn = manager.create_iam_role()
        if role_arn:
            print(f"✅ IAM Role created successfully: {role_arn}")
            
            print("\n2. Creating Security Group...")
            security_group_id = manager.create_security_group()
            if security_group_id:
                print(f"✅ Security Group created successfully: {security_group_id}")
                
                print("\n3. Creating Redshift Cluster...")
                if manager.create_redshift_cluster(role_arn, security_group_id):
                    print("✅ Redshift cluster creation initiated")
                    
                    print("\n4. Waiting for cluster to become available...")
                    if manager._wait_for_cluster_available():
                        endpoint = manager.get_cluster_endpoint()
                        print("\n=== Cluster Creation Complete ===")
                        print(f"✅ Cluster Endpoint: {endpoint}")
                        print(f"✅ Database Name: {manager.config.get('DWH', 'DWH_DB')}")
                        print(f"✅ Port: {manager.config.get('DWH', 'DWH_PORT')}")
                        print("\nYou can now connect to your cluster using these credentials")
                    else:
                        print("❌ Cluster creation timed out")
                else:
                    print("❌ Failed to create Redshift cluster")
            else:
                print("❌ Failed to create Security Group")
        else:
            print("❌ Failed to create IAM Role")

    if args.delete:
        print("\nStarting infrastructure cleanup...")
        if manager.cleanup():
            print("✅ All resources cleaned up successfully")
        else:
            print("❌ Some resources may not have been cleaned up properly")

    if args.connect:
        print("\nTesting connection to Redshift cluster...")
        manager.connect_to_redshift()

    print("\nCheck redshift_infra.log for detailed logs")

if __name__ == "__main__":
    main()

""" 

python redshift_manager.py --create --config cluster.config


python redshift_manager.py --connect --config cluster.config

python redshift_manager.py --delete --config cluster.config
"""