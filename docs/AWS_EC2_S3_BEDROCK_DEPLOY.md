# AWS EC2 + S3 Deployment Runbook

This is the lowest-cost deployment path for the NAS proof of concept: one EC2 instance running Docker Compose, one S3 bucket for uploads/artifacts, and optional Amazon Bedrock for AI-assisted address parsing.

## 1. Choose the first deployment mode

Start with deterministic parsing unless you specifically need AI parsing on day one.

| Mode | What to set | Cost behavior |
|---|---|---|
| Cheapest first deploy | `ADDRESS_PARSER_MODE=deterministic` and `BEDROCK_ADDRESS_PARSER_ENABLED=false` | No Bedrock token cost |
| Bedrock enabled | `ADDRESS_PARSER_MODE=ai_required` and `BEDROCK_ADDRESS_PARSER_ENABLED=true` | Pay per token used |

Recommended first target:

- Region: `ap-southeast-1` for EC2 and S3.
- Instance: `t3a.large` or `t3.large` with 8 GB RAM.
- Disk: gp3 EBS, 80-120 GB.
- Public access: port `80` only for the app, port `22` only from your IP.
- No RDS, no managed OpenSearch, no ALB, no NAT Gateway for the first POC.

## 2. Create the S3 bucket

In the AWS Console:

1. Open S3.
2. Create a bucket, for example `nas-poc-<your-name>`.
3. Use the same region as EC2, for example `ap-southeast-1`.
4. Keep public access blocked.
5. Enable default encryption.

The app does not need the bucket to be public. The backend signs and reads objects through AWS credentials.

## 3. Create the EC2 IAM role

Create an IAM policy like this, replacing the bucket name:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "NasS3BucketList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::nas-poc-<your-name>"
    },
    {
      "Sid": "NasS3Objects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::nas-poc-<your-name>/*"
    }
  ]
}
```

Then:

1. Create an IAM role for EC2.
2. Attach this S3 policy.
3. If using Bedrock, also attach a policy allowing `bedrock:InvokeModel` and `bedrock:InvokeModelWithResponseStream` for the chosen model.

Prefer this role over putting `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`.

## 4. Launch the EC2 instance

Use:

- AMI: Ubuntu Server 24.04 LTS or 22.04 LTS.
- Instance type: `t3a.large` if available, otherwise `t3.large`.
- Storage: gp3, 80-120 GB.
- IAM role: the role from step 3.
- Security group:
  - Inbound `22/tcp` from your own IP only.
  - Inbound `80/tcp` from the users who need the portal.
  - Do not open Postgres, OpenSearch, Valkey, or Docker ports publicly.

After the instance starts, SSH in:

```bash
ssh -i <key.pem> ubuntu@<ec2-public-ip>
```

## 5. Install Docker on EC2

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker ubuntu
```

Log out and back in so the `docker` group applies.

## 6. Set the OpenSearch kernel setting

OpenSearch needs this on the host:

```bash
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
```

## 7. Put the repo on the server

For a private repo, use your preferred GitHub deploy key or SSH key:

```bash
git clone <repo-url> nas
cd nas
```

For a quick manual copy, upload the project directory with `scp` or `rsync`, excluding `.venv`, logs, and generated outputs.

## 8. Create the production `.env`

On EC2:

```bash
cp .env.example .env
nano .env
```

Minimum values for cheapest mode:

```env
OBJECT_STORE_BUCKET=nas-poc-<your-name>
AWS_REGION=ap-southeast-1
OBJECT_STORE_ENDPOINT=
OBJECT_STORE_PUBLIC_ENDPOINT=
OBJECT_STORE_SECURE=true
OBJECT_STORE_PUBLIC_SECURE=true
OBJECT_STORE_USE_PATH_STYLE=false
OBJECT_STORE_AUTO_CREATE_BUCKET=false
OBJECT_STORE_MANAGE_CORS=false
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=

CORS_ALLOW_ORIGINS=http://<ec2-public-ip>
NAS_AUDIT_LOG=logs/nas_audit.log
STRICT_ENV_CHECK=true
STRICT_LOOKUP_DB_CHECK=true
INGEST_STALE_CLAIM_SECONDS=300
INGEST_STALE_CLAIM_RECOVERY_INTERVAL_SECONDS=30

ADDRESS_PARSER_MODE=deterministic
BEDROCK_ADDRESS_PARSER_ENABLED=false
BEDROCK_AWS_REGION=
BEDROCK_CLAUDE_MODEL_ID=anthropic.claude-3-haiku-20240307-v1:0
BEDROCK_ADDRESS_BATCH_SIZE=25
OPENAI_API_KEY=
```

If you later add a domain, change `CORS_ALLOW_ORIGINS` to `http://your-domain` or `https://your-domain`.

## 9. Start the stack

```bash
docker compose up -d --build
```

Watch startup:

```bash
docker compose ps
docker compose logs -f api
```

The first boot runs migrations and bootstraps lookup tables from `data/lookups_clean/`.

## 10. Verify the app

From EC2:

```bash
curl http://localhost/health
curl http://localhost/api/v1/health
```

From your laptop:

```bash
curl http://<ec2-public-ip>/health
curl http://<ec2-public-ip>/api/v1/health
```

Then open:

```text
http://<ec2-public-ip>
```

## 11. Enable Bedrock later

First confirm the model is available in the Bedrock region you want to use, then request model access in the Bedrock console.

Update `.env`:

```env
ADDRESS_PARSER_MODE=ai_required
BEDROCK_ADDRESS_PARSER_ENABLED=true
BEDROCK_AWS_REGION=<bedrock-region>
BEDROCK_CLAUDE_MODEL_ID=<model-id>
```

Examples:

```env
BEDROCK_AWS_REGION=us-east-1
BEDROCK_CLAUDE_MODEL_ID=anthropic.claude-3-haiku-20240307-v1:0
```

or, if available for your account and region:

```env
BEDROCK_AWS_REGION=us-west-2
BEDROCK_CLAUDE_MODEL_ID=anthropic.claude-3-5-haiku-20241022-v1:0
```

Restart the API and worker:

```bash
docker compose up -d --build api worker
```

## 12. Basic operations

View running services:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs --tail=100 api
docker compose logs --tail=100 worker
docker compose logs --tail=100 opensearch
```

Restart after changing `.env`:

```bash
docker compose up -d
```

Back up local Docker volumes before risky changes:

```bash
docker run --rm -v nas_postgres_data:/volume -v "$PWD:/backup" alpine tar czf /backup/postgres_data_backup.tgz -C /volume .
docker run --rm -v nas_opensearch_data:/volume -v "$PWD:/backup" alpine tar czf /backup/opensearch_data_backup.tgz -C /volume .
```

## 13. When to move beyond the cheap setup

Move to managed services when:

- You need reliable backups and point-in-time restore: move Postgres to RDS.
- Search data becomes important and large: move OpenSearch to Amazon OpenSearch Service.
- Multiple users need stable production access: add Route 53, HTTPS, and either Caddy/Nginx certbot on EC2 or an ALB.
- You need zero-downtime deploys or multiple instances: move containers to ECS.

