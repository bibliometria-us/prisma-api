import os

def get_secret(secret_name, default=None):
    secret_path = f"/run/secrets/{secret_name}"
    if os.path.exists(secret_path):
        with open(secret_path, "r") as f:
            return f.read().strip()
    return os.getenv(secret_name.upper(), default)

def get_flask_secret_key():
    return get_secret("flask_secret_key", os.urandom(24).hex())