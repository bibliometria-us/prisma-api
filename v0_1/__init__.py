from flask import Blueprint
from v0_1.routes.test.main import test_bp

v0_1_bp = Blueprint("v0_1", __name__, url_prefix="/v0.1")
v0_1_bp.register_blueprint(test_bp)
