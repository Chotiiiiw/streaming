resource "aws_security_group" "flink" {
  name_prefix = "${var.project_name}-${var.environment}-flink-"
  description = "Network access for Managed Flink"
  vpc_id      = aws_vpc.main.id

  revoke_rules_on_delete = true

  tags = {
    Name = "${var.project_name}-${var.environment}-flink"
  }
}

resource "aws_security_group" "msk" {
  name_prefix = "${var.project_name}-${var.environment}-msk-"
  description = "Network access for MSK Serverless"
  vpc_id      = aws_vpc.main.id

  revoke_rules_on_delete = true

  tags = {
    Name = "${var.project_name}-${var.environment}-msk"
  }
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_flink" {
  security_group_id = aws_security_group.msk.id

  referenced_security_group_id = aws_security_group.flink.id

  from_port   = 9098
  to_port     = 9098
  ip_protocol = "tcp"

  description = "Allow Managed Flink to connect using MSK IAM"
}

resource "aws_vpc_security_group_egress_rule" "flink_to_msk" {
  security_group_id = aws_security_group.flink.id

  referenced_security_group_id = aws_security_group.msk.id

  from_port   = 9098
  to_port     = 9098
  ip_protocol = "tcp"

  description = "Allow Managed Flink to reach MSK IAM endpoint"
}

resource "aws_vpc_security_group_egress_rule" "flink_to_s3" {
  security_group_id = aws_security_group.flink.id

  prefix_list_id = aws_vpc_endpoint.s3.prefix_list_id

  from_port   = 443
  to_port     = 443
  ip_protocol = "tcp"

  description = "Allow Managed Flink to reach S3 through the gateway endpoint"
}

resource "aws_vpc_security_group_egress_rule" "msk_outbound" {
  security_group_id = aws_security_group.msk.id

  cidr_ipv4   = "0.0.0.0/0"
  ip_protocol = "-1"

  description = "Allow managed MSK service outbound traffic"
}