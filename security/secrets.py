import os


def get_secret(secret_name: str, default: str | None = None) -> str | None:
    secret_path = f"/run/secrets/{secret_name}"
    if os.path.exists(secret_path):
        with open(secret_path, "r") as f:
            return f.read().strip()

    env = os.getenv(secret_name.upper())
    if env:
        return env

    return None


def get_flask_secret_key() -> str | None:
    return get_secret("flask_secret_key", os.urandom(24).hex())
