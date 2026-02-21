from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "django-insecure-key"  # noqa: S105

DEBUG = True

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django_tasks_inprocess",
    "tests",
]


USE_TZ = True

TASKS = {
    "default": {
        "BACKEND": "django_tasks_inprocess.InProcessTaskBackend",
        "QUEUES": ["default", "queue-1"],
    }
}
