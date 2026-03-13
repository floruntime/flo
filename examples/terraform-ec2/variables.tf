variable "region" {
  type        = string
  description = "AWS region"
  default     = "us-east-1"
}

variable "az_suffix" {
  type        = string
  description = "Availability zone suffix (e.g. 'a', 'b')"
  default     = "a"
}

variable "name" {
  type        = string
  description = "Name prefix for all resources"
  default     = "flo"
}

variable "instance_type" {
  type        = string
  description = "EC2 instance type. c6i/c7i recommended (CPU-bound, thread-per-shard)."
  default     = "c6i.xlarge"
}

variable "key_name" {
  type        = string
  description = "Existing EC2 key pair name for SSH access"
}

variable "allowed_cidr" {
  type        = string
  description = "CIDR allowed to access SSH and Flo ports"
  default     = "0.0.0.0/0"
}

variable "ami_id" {
  type        = string
  default     = null
  description = "Custom AMI ID (defaults to Ubuntu 24.04)"
}

variable "flo_version" {
  type        = string
  default     = ""
  description = "Flo release version (e.g. v0.1.0). Empty = latest."
}

variable "shard_count" {
  type        = number
  default     = 0
  description = "Number of shards (0 = auto-detect from CPU count)"
}

# --- Data volume ---

variable "data_volume_size_gb" {
  type        = number
  default     = 50
  description = "Size of the dedicated EBS data volume in GiB"
}

variable "data_volume_type" {
  type        = string
  default     = "gp3"
  description = "EBS volume type for data (gp3, io2)"
}

variable "data_volume_iops" {
  type        = number
  default     = 3000
  description = "Provisioned IOPS for gp3/io2 data volume"
}

variable "data_volume_throughput" {
  type        = number
  default     = 250
  description = "Throughput in MiB/s for gp3 data volume"
}
