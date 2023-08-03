from flask import request, Response
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from security.api_key import (comprobar_api_key)
import utils.format as format
import config.global_config as gconfig


investigador_namespace = Namespace(
    'investigador', description="")

global_responses = gconfig.responses

global_params = gconfig.params
