variable "credentials" {
  description = "Project Credentials"
  default     = "./keys/my-creds.json"
}

variable "project_id" {
  description = "Project ID"
  default     = "primal-seeker-484820-a0"
}

variable "region" {
  description = "Project Region"
  default     = "europe-west3"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket name"
  default     = "dez-terraform-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}