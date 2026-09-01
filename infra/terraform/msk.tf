resource "aws_msk_serverless_cluster" "main" {
  cluster_name = "${var.project_name}-${var.environment}-msk"

  vpc_config {
    subnet_ids = aws_subnet.private[*].id

    security_group_ids = [
      aws_security_group.msk.id
    ]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-msk"
  }
}