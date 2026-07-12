---
confluence_page_id: "98586"
---
# Deploying the Medallion Data Stack to Dagster Cloud

This guide details the steps and configuration patterns required to deploy the Bristol Air Quality data stack to **Dagster Cloud**. 

---

## 1. Choosing a Deployment Model

To deploy your Software-Defined Assets (SDAs), you must choose between two models based on where your database engines are hosted:

| Deployment Model | Control Plane (UI, Schedules) | Execution Tier (ETL, dbt, Python) | Database Connection Requirements | Recommendation |
|---|---|---|---|---|
| **Hybrid** | Hosted by Dagster Cloud | Hosted on your private infrastructure (Local, AWS ECS, GKE) | Pipelines connect securely within your private subnet or local network. | **Highly Recommended** (Allows orchestration of your Docker-based local/private databases without public internet exposure). |
| **Serverless** | Hosted by Dagster Cloud | Managed sandbox containers run by Dagster | Databases must be publicly accessible or configured with whitelist rules to allow Dagster Cloud IPs. | **Cloud-Only** (Requires moving your PostgreSQL and MongoDB instances to public clouds like AWS RDS and MongoDB Atlas). |

---

## 2. Hybrid Deployment Setup (Recommended)

In a Hybrid deployment, the Dagster Cloud UI orchestrates the pipeline, but a lightweight agent running inside your network executes the runs. 

### Step 1: Generate a Dagster Cloud API Token
1. Sign in to your **Dagster Cloud** account.
2. Navigate to **Cloud Settings** -> **Tokens**.
3. Generate a new **Agent Token** and copy the value.

### Step 2: Configure Code Location & Container Registry
You can automate the Docker container compilation, registry tagging, remote pushing, and code location configuration using the onboarding script:
```bash
python scripts/prep_dagster_cloud.py
```
*(This interactive script will guide you through entering your registry, compile the Dockerfile, write `dagster_cloud.yaml` in the root, and push the image).*

> [!TIP]
> If your Docker Hub username is different from your GitHub username, you can add it to your local `.env` file first:
> ```env
> DOCKER_USERNAME=your_actual_docker_username
> ```
> The script will automatically read this variable and use it as the default choice in the prompt!

#### Alternative (Manual Setup):
1. Build, tag, and push your container image manually:
   ```bash
   docker build -t bristol-air-pipeline:latest -f docker/Dockerfile.pipeline .
   docker tag bristol-air-pipeline:latest your-registry/bristol-air-pipeline:latest
   docker push your-registry/bristol-air-pipeline:latest
   ```
2. Create `dagster_cloud.yaml` in the project root:
   ```yaml
   # dagster_cloud.yaml
   locations:
     - location_name: bristol-air-quality-pipeline
       image: your-registry/bristol-air-pipeline:latest
       code_source:
         package_name: dagster_orch
   ```

### Step 3: Run the Dagster Agent Locally

To allow Dagster Cloud to communicate with your local Docker network and trigger runs on your machine, you must configure and start the Dagster Cloud Agent.

1. **Configure local agent properties:**
   Create a `config/dagster_agent.yaml` file to tell the instance to boot as a cloud agent instance, target the `prod` deployment, and launch user code containers within the `bristol-air-net` network:
   ```yaml
   # config/dagster_agent.yaml
   instance_class:
     module: dagster_cloud.instance
     class: DagsterCloudAgentInstance

   dagster_cloud_api:
     agent_token:
       env: DAGSTER_CLOUD_API_TOKEN
     deployment: prod

   user_code_launcher:
     module: dagster_cloud.workspace.docker
     class: DockerUserCodeLauncher
     config:
       networks:
         - bristol-air-net
   ```

2. **Set your Agent Token:**
   Add your Dagster Cloud API Token to your local `.env` file:
   ```env
   DAGSTER_CLOUD_API_TOKEN=your-copied-agent-token
   ```

3. **Start the agent using the built-in docker-compose profile:**
   ```bash
   docker compose --profile agent up -d
   ```
   *(Using Docker Compose automatically mounts `dagster_agent.yaml` as the container instance configuration and runs the agent securely).*

#### Alternative: Raw Docker CLI
If you prefer to start it via raw Docker commands, mount your local `dagster_agent.yaml` file into the container's app workspace and run:

* **Windows (Git Bash):**
  ```bash
  docker run -d \
    --name dagster-cloud-agent \
    --network bristol-air-net \
    -v //./pipe/docker_engine:/var/run/docker.sock \
    -v "$PWD/config/dagster_agent.yaml:/opt/dagster/app/dagster.yaml:ro" \
    -e DAGSTER_CLOUD_API_TOKEN=$DAGSTER_CLOUD_API_TOKEN \
    dagster/dagster-cloud-agent:latest \
    dagster-cloud agent run /opt/dagster/app
  ```
* **macOS / Linux:**
  ```bash
  docker run -d \
    --name dagster-cloud-agent \
    --network bristol-air-net \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$PWD/config/dagster_agent.yaml:/opt/dagster/app/dagster.yaml:ro" \
    -e DAGSTER_CLOUD_API_TOKEN=$DAGSTER_CLOUD_API_TOKEN \
    dagster/dagster-cloud-agent:latest \
    dagster-cloud agent run /opt/dagster/app
  ```

---

## 3. Serverless Deployment Setup

If you migrate your databases to the cloud (e.g. AWS RDS PostgreSQL and MongoDB Atlas), you can let Dagster Cloud handle all compute.

### Step 1: Create `dagster_cloud.yaml`
```yaml
# dagster_cloud.yaml
locations:
  - location_name: bristol-air-quality-pipeline
    code_source:
      package_name: dagster_orch
```

### Step 2: Configure secrets in GitHub
Add the following secrets to your GitHub repository under **Settings** -> **Secrets and variables** -> **Actions**:
* `DAGSTER_CLOUD_ORGANIZATION_ID`: Your Dagster Cloud organization identifier.
* `DAGSTER_CLOUD_API_TOKEN`: Your Dagster Cloud User Token (with deploy permissions).

### Step 3: Add GitHub Actions Deployment Workflow
Create `.github/workflows/dagster-cloud-deploy.yml` to build and deploy your code location automatically on pushes to `main`:

```yaml
name: Deploy to Dagster Cloud (Serverless)
on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v3

      - name: Deploy to Dagster Cloud
        uses: dagster-io/dagster-cloud-action/actions/serverless_deploy@v0.1
        with:
          organization_id: ${{ secrets.DAGSTER_CLOUD_ORGANIZATION_ID }}
          api_token: ${{ secrets.DAGSTER_CLOUD_API_TOKEN }}
```

---

## 4. Verifying Deployment
Once the deployment finishes:
1. Navigate to your Dagster Cloud dashboard (`https://dagster.cloud/your-org`).
2. Go to **Deployment** -> **Code locations**.
3. You should see `bristol-air-quality-pipeline` loaded successfully with the complete software-defined asset lineage!
