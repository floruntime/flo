terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# --- AMI lookup ---

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

# --- Security group ---

resource "aws_security_group" "flo" {
  name        = "${var.name}-sg"
  description = "Flo server security group"

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
    description = "SSH"
  }

  # Wire protocol (clients, SDKs, CLI)
  ingress {
    from_port   = 9000
    to_port     = 9000
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
    description = "Flo wire protocol"
  }

  # Dashboard + REST API
  ingress {
    from_port   = 9002
    to_port     = 9002
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
    description = "Flo dashboard"
  }

  # Metrics (Prometheus scrape)
  ingress {
    from_port   = 9001
    to_port     = 9001
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
    description = "Flo metrics"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name}-sg"
  }
}

# --- EBS data volume ---
# Separate from root so it survives instance replacement and can be snapshotted.

resource "aws_ebs_volume" "flo_data" {
  availability_zone = "${var.region}${var.az_suffix}"
  size              = var.data_volume_size_gb
  type              = var.data_volume_type
  iops              = var.data_volume_type == "gp3" ? var.data_volume_iops : null
  throughput        = var.data_volume_type == "gp3" ? var.data_volume_throughput : null

  tags = {
    Name = "${var.name}-data"
  }
}

# --- EC2 instance ---

resource "aws_instance" "flo" {
  ami                         = coalesce(var.ami_id, data.aws_ami.ubuntu.id)
  instance_type               = var.instance_type
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.flo.id]
  associate_public_ip_address = true
  availability_zone           = "${var.region}${var.az_suffix}"

  # Root volume: OS + binary only
  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    flo_version       = var.flo_version
    data_device       = "/dev/xvdf"
    data_mount        = "/var/lib/flo"
    shard_count       = var.shard_count
  })

  tags = {
    Name = var.name
  }
}

resource "aws_volume_attachment" "flo_data" {
  device_name = "/dev/xvdf"
  volume_id   = aws_ebs_volume.flo_data.id
  instance_id = aws_instance.flo.id
}
