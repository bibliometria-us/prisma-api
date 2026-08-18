from flask import Blueprint

test_bp = Blueprint("test", __name__, url_prefix="/test")


@test_bp.route("/hello-world", methods=["GET"])
def get_test():
    return {"message": "Hello, World!"}
