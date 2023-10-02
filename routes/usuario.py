from flask import request, session, redirect, url_for
from flask_restx import Namespace, Resource
from db.conexion import BaseDatos
from config import user as user_data
import utils.response as response
import security.api_key as api_key
import security.login as login

usuario_namespace = Namespace('usuario', description="Datos del usuario")

anonymous_user = user_data.anonymous_user_data
dummy_user = user_data.dummy_user


def get_user_data():
    data = anonymous_user
    if 'samlUserdata' in session:
        data = session['samlUserdata']
    result = data

    # Carga usuario ficticio si existe (para versión local)
    if dummy_user and login.is_logged_in():
        result = dummy_user

    return result


def get_api_key():
    result = None

    if login.is_logged_in():
        mail = session['samlUserdata']['mail'][0]
        result = api_key.api_key_from_user(mail)

    return result


def update_api_key():
    result = None

    if login.is_logged_in():
        mail = session['samlUserdata']['mail'][0]
        result = api_key.create_api_key(mail)

    return result


@usuario_namespace.route('/datos/')
class DatosUsuario(Resource):
    def get(self):
        headers = request.headers
        args = request.args

        accept_type = args.get('salida', headers.get(
            'Accept', 'application/json'))
        data = get_user_data()
        data["api_key"] = get_api_key()
        return response.generate_response(data=data,
                                          output_types=["json", "xml"],
                                          accept_type=accept_type,
                                          nested={},
                                          namespace=usuario_namespace,
                                          dict_selectable_column="id",
                                          object_name="usuario",
                                          xml_root_name=None,)


@usuario_namespace.route('/login/')
class LoginUsuario(Resource):
    def get(self):
        if dummy_user:
            session['login'] = True
            session['samlUserdata'] = dummy_user
            return redirect("/")
        return redirect("/auth/?sso")


@usuario_namespace.route('/logout/')
class LogoutUsuario(Resource):
    def get(self):
        session.clear()
        session['login'] = False
        return redirect("/")


@usuario_namespace.route('/api_key/update')
class UpdateApiKeyUsuario(Resource):
    def get(self):
        if login.is_logged_in():
            update_api_key()
        return redirect("/")
