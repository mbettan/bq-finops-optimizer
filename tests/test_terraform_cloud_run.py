"""Guards for the Terraform + Cloud Build Cloud Run deploy stack."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TERRAFORM = ROOT / "terraform"

REQUIRED_TF_FILES = [
    "versions.tf",
    "providers.tf",
    "variables.tf",
    "apis.tf",
    "service_account.tf",
    "iam_project.tf",
    "iam_org.tf",
    "iam_folder.tf",
    "iam_cloudbuild.tf",
    "iam_deployer.tf",
    "iam_invoker.tf",
    "artifact_registry.tf",
    "cloud_run.tf",
    "outputs.tf",
    "terraform.tfvars.example",
]

FORBIDDEN_SUBSTRINGS = [
    "val.bezrukov",
    "prftgcpdemo.ai",
    "vbe-bq-finops-optimizer",
    "967244177276",
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _tracked_deploy_files():
    files = [ROOT / "cloudbuild.yaml", ROOT / ".gitignore", ROOT / "README.md"]
    files.extend(TERRAFORM / name for name in REQUIRED_TF_FILES)
    return files


def test_required_terraform_and_cloudbuild_files_exist():
    missing = [name for name in REQUIRED_TF_FILES if not (TERRAFORM / name).is_file()]
    assert missing == [], f"missing terraform files: {missing}"
    assert (ROOT / "cloudbuild.yaml").is_file()


def test_private_tfvars_and_state_are_not_committed():
    assert not (TERRAFORM / "terraform.tfvars").exists()
    assert not list(TERRAFORM.glob("*.tfstate*"))


def test_examples_and_defaults_have_no_customer_secrets():
    hits = []
    for path in _tracked_deploy_files():
        text = _read(path)
        for needle in FORBIDDEN_SUBSTRINGS:
            if needle in text:
                hits.append(f"{path.relative_to(ROOT)}: {needle}")
    assert hits == [], f"sanitization failures: {hits}"


def test_iam_scope_supports_organization_and_folder():
    variables = _read(TERRAFORM / "variables.tf")
    assert 'variable "iam_scope"' in variables
    assert 'variable "folder_id"' in variables
    assert 'variable "org_id"' in variables
    org = _read(TERRAFORM / "iam_org.tf")
    folder = _read(TERRAFORM / "iam_folder.tf")
    assert "google_organization_iam_member" in org
    assert "google_folder_iam_member" in folder
    assert 'iam_scope == "organization"' in org
    assert 'iam_scope == "folder"' in folder


def test_invoker_and_deployer_defaults_are_empty():
    variables = _read(TERRAFORM / "variables.tf")
    assert 'variable "invoker_members"' in variables
    assert 'variable "deployer_members"' in variables
    assert "default     = []" in variables


def test_cloud_run_is_authenticated_and_ignores_image_rollouts():
    cloud_run = _read(TERRAFORM / "cloud_run.tf")
    assert "INGRESS_TRAFFIC_ALL" in cloud_run
    assert "AUTH_ENFORCED_UPSTREAM" in cloud_run
    assert "ignore_changes" in cloud_run
    assert "template[0].containers[0].image" in cloud_run
    cloudbuild = _read(ROOT / "cloudbuild.yaml")
    assert "--no-allow-unauthenticated" in cloudbuild
    assert "AUTH_ENFORCED_UPSTREAM=true" in cloudbuild
    assert "$BUILD_ID" in cloudbuild


def test_gitignore_excludes_terraform_state_and_private_tfvars():
    gitignore = _read(ROOT / ".gitignore")
    for entry in (
        "terraform/.terraform/",
        "terraform/.terraform.lock.hcl",
        "terraform/*.tfstate",
        "terraform/terraform.tfvars",
    ):
        assert entry in gitignore, f"missing gitignore entry: {entry}"
