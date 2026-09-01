resource "aws_cloudwatch_log_group" "flink" {
  name = "/aws/managed-flink/${var.project_name}-${var.environment}"

  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-${var.environment}-flink-logs"
  }
}

resource "aws_cloudwatch_log_stream" "flink" {
  name = "application"

  log_group_name = aws_cloudwatch_log_group.flink.name
}