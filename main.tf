variable "aws_region" {
  default = "eu-west-1" //You can set your preferred region here
}

provider "aws" {
  region = "${var.aws_region}"
}

resource "random_id" "bucket" {
  byte_length = 8
}

resource "aws_s3_bucket" "objects" {
  bucket = "objects-${random_id.bucket.hex}"
}

module "api" {
  source = "github.com/nerdalize/rotor//rotortf"
  aws_region = "${var.aws_region}"

  func_name = "objects-api"
  func_description = "append-only interface for s3 bucket '${aws_s3_bucket.objects.bucket}'"
  func_zip_path = "build.zip"
  func_env = {
      S3_BUCKET = "${aws_s3_bucket.objects.bucket}"
  }

  func_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "logs:*"
      ],
      "Effect": "Allow",
      "Resource": "*"
    },
    {
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Effect": "Allow",
      "Resource":["arn:aws:s3:::${aws_s3_bucket.objects.bucket}/*"]
    }
  ]
}
EOF

  api_name = "Object API"
  api_description = "An append-only object store with an s3 backend"
}

resource "aws_api_gateway_deployment" "api" {
  rest_api_id = "${module.api.rest_api_id}"
  stage_name = "v1"
  stage_description = "v1 (${module.api.aws_api_gateway_method})" //THIS HACK IS MANDATORY
}

output "api_endpoint" {
  value = "https://${module.api.rest_api_id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_deployment.api.stage_name}"
}
