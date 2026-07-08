# Cloud Infrastructure Deployment Roadmap

This document outlines the provisioning guidelines, service architecture, and CI/CD pipelines required to deploy the three-tier Bristol Air Quality data stack to production in a public cloud (AWS).

---

## 1. Production Architecture Overview

The production deployment replaces local containerized databases with fully managed cloud services to guarantee high availability, scaling, automatic backups, and disaster recovery.

```mermaid
flowchart TD
    %% Users
    ENG[Engineers & Client Apps] -->|HTTPS| DAG_UI[Dagster Cloud / Web Console]

    %% Compute Layer
    subgraph Compute: AWS ECS (Fargate)
        DAG_AGENT[Dagster Daemon Agent]
        PIPELINE[ETL Execution Container]
    end

    %% Storage Layer
    subgraph Storage: Managed Databases
        RDS[(Amazon RDS: PostgreSQL)]
        ATLAS[(MongoDB Atlas)]
    end

    %% Secret Management
    SECRETS[AWS Secrets Manager] -.->|Injects Credentials| PIPELINE & DAG_AGENT

    %% Connection Lines
    DAG_UI -->|Trigger Job| DAG_AGENT
    DAG_AGENT -->|Orchestrate Task| PIPELINE
    PIPELINE -->|Transformed Bulk Load| RDS
    RDS -->|dbt transformation models| RDS
    PIPELINE -->|Denormalized Stream| ATLAS
```

---

## 2. Infrastructure Provisioning (Terraform)

We utilize Infrastructure as Code (IaC) via **Terraform** to provision the cloud infrastructure across isolated Dev, Staging, and Production environments.

### Relational Storage (PostgreSQL)
* **Service**: Amazon RDS PostgreSQL (v16+)
* **Instance Class**: `db.t4g.medium` (Dev/Staging), `db.r6g.large` (Production)
* **Configuration**:
  - Multi-AZ enabled for Production (automatic failover).
  - Storage Auto-Scaling enabled (up to 500 GB).
  - Daily automated snapshots with 30-day retention.

### Document Serving Cache (MongoDB)
* **Service**: MongoDB Atlas (Managed Service)
* **Tier**: M10 (Dev/Staging), M30 (Production)
* **Connectivity**: VPC Peering or AWS PrivateLink between the ECS VPC and MongoDB Atlas to bypass public internet traffic.

### Container Hosting (ECS Fargate)
* **Service**: AWS ECS with AWS Fargate launch type (Serverless CPU/RAM allocation).
* **Task Definitions**:
  - `data-generator-task`: 0.25 vCPU, 512 MB RAM (ad-hoc runs).
  - `etl-pipeline-task`: 1 vCPU, 2 GB RAM (scheduled batch execution).
* **Network**: Private subnets with NAT Gateways to restrict internet-bound ingress.

---

## 3. Security & Secret Management

* **Zero Hardcoded Credentials**: Database passwords, hosts, and Mongo URIs are stored inside **AWS Secrets Manager**.
* **IAM Roles**: The ECS Task Execution Role is granted the narrowest IAM policy:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "secretsmanager:GetSecretValue"
        ],
        "Resource": [
          "arn:aws:secretsmanager:region:account:secret:bristol-air-db-*"
        ]
      }
    ]
  }
  ```
* **Network Security Groups**:
  - `RDS Security Group`: Allows ingress only on port `5432` from the `ETL Security Group`.
  - `MongoDB Atlas`: Whitelists only the NAT Gateway elastic IPs of the ECS VPC.

---

## 4. CI/CD Deployment Pipeline (GitHub Actions)

We automate tests, Docker image building, and Fargate deployment using GitHub Actions:

```yaml
name: Production Deployment Pipeline

on:
  push:
    branches:
      - main

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install uv and run tests
        run: |
          curl -LsSf https://astral.sh/uv/install.sh | sh
          uv sync --group pipeline --group dev
          uv run pytest tests/

  build-and-push:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - name: Configure AWS Credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1

      - name: Login to Amazon ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build & Push ETL Image
        run: |
          docker build -t 123456789012.dkr.ecr.us-east-1.amazonaws.com/etl-pipeline:latest -f docker/Dockerfile.pipeline .
          docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/etl-pipeline:latest

  deploy:
    needs: build-and-push
    runs-on: ubuntu-latest
    steps:
      - name: Force ECS Fargate Deployment
        run: |
          aws ecs update-service --cluster bristol-air-cluster --service etl-pipeline-service --force-new-deployment
