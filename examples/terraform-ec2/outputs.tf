output "instance_id" {
  value = aws_instance.flo.id
}

output "public_ip" {
  value = aws_instance.flo.public_ip
}

output "wire_endpoint" {
  description = "Flo wire protocol endpoint (for SDKs and CLI)"
  value       = "${aws_instance.flo.public_ip}:9000"
}

output "dashboard_url" {
  description = "Flo dashboard URL"
  value       = "http://${aws_instance.flo.public_ip}:9002"
}

output "metrics_url" {
  description = "Prometheus metrics endpoint"
  value       = "http://${aws_instance.flo.public_ip}:9001/metrics"
}

output "ssh_command" {
  value = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.flo.public_ip}"
}
