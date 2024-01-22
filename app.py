# Import from the new location
from api_logging.sso_data import store_sso_data
import config.local_config as local_config
from api_logging.log_request import log_request
from routes import (investigador, publicacion, fuente, proyecto, instituto,
                    departamento, grupo, prog_doctorado, editorial, resultado, usuario)
from routes.informes.main import informe_namespace
from routes.carga.main import carga_namespace
import os
from flask import Flask, request, redirect, url_for, session, render_template, make_response, Blueprint
from flask_restx import Api
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.utils import OneLogin_Saml2_Utils
import logging
from security.protected_routes import mandatory_auth_endpoints
from celery import Celery

prisma_base_url = "https://prisma.us.es"

app = Flask(__name__)

app.config['SECRET_KEY'] = 'onelogindemopytoolkit'
app.config['SAML_PATH'] = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'saml')
api_bp = Blueprint("api", __name__, url_prefix=local_config.api_base_path)
app.config.from_object('celery_config')

api = Api(api_bp, version="1.0", title="Prisma API")
logging.basicConfig(level=logging.DEBUG)

api.add_namespace(investigador.investigador_namespace)
api.add_namespace(publicacion.publicacion_namespace)
api.add_namespace(fuente.fuente_namespace)
api.add_namespace(proyecto.proyecto_namespace)
api.add_namespace(instituto.instituto_namespace)
api.add_namespace(departamento.departamento_namespace)
api.add_namespace(grupo.grupo_namespace)
api.add_namespace(prog_doctorado.doctorado_namespace)
api.add_namespace(editorial.editorial_namespace)
api.add_namespace(resultado.resultado_namespace)
api.add_namespace(usuario.usuario_namespace)
api.add_namespace(informe_namespace)
api.add_namespace(carga_namespace)

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
celery.set_default()

@app.before_request
def auth_check():
    args = request.args
    requires_mandatory_auth = request.endpoint in mandatory_auth_endpoints
    try:
        if requires_mandatory_auth:
            if not session.get("samlUserdata"):
                return redirect(url_for("api.login", redirect_url = request.url))
            if not session.get("samlUserdata"):
                raise Exception
    except:
        return {'message': 'No autorizado'}, 401


@app.after_request
def after_request(response):
    route = request.path
    args = dict(request.args)
    response_code = response.status_code
    user = "anon"
    if session.get("samlUserdata"):
        user = session["samlUserdata"]["mail"][0].split("@")[0]

    log_request(route, args, response_code, user)

    return response

# ERRORES GLOBALES
@api.errorhandler(ValueError)  # Custom error handler for ValueError
def handle_invalid_accept_header(error):
    """Error handler for Invalid Accept header."""
    return {'message': 'Formato de salida no soportado'}, 406


# SAML

def init_saml_auth(req):
    auth = OneLogin_Saml2_Auth(req, custom_base_path=app.config['SAML_PATH'])
    return auth


def prepare_flask_request(request):
    # If server is behind proxys or balancers use the HTTP_X_FORWARDED fields
    return {
        'https': 'on',
        'http_host': request.host,
        'script_name': request.full_path,
        'get_data': request.args.copy(),
        # Uncomment if using ADFS as IdP, https://github.com/onelogin/python-saml/pull/144
        # 'lowercase_urlencoding': True,
        'post_data': request.form.copy()
    }


@api_bp.route('/auth/', methods=['GET', 'POST'], endpoint="auth")
def index():
    req = prepare_flask_request(request)
    auth = init_saml_auth(req)
    errors = []
    error_reason = None
    not_auth_warn = False
    success_slo = False
    attributes = False
    paint_logout = False
    redirect_url = request.args.get("redirect_url") or local_config.prisma_url
    if 'sso' in request.args:
        return redirect(auth.login(return_to=redirect_url))
        # If AuthNRequest ID need to be stored in order to later validate it, do instead
        # sso_built_url = auth.login()
        # request.session['AuthNRequestID'] = auth.get_last_request_id()
        # return redirect(sso_built_url)
    elif 'sso2' in request.args:
        return_to = '%sattrs/' % request.host_url
        return redirect(auth.login(return_to))
    elif 'slo' in request.args:
        name_id = session_index = name_id_format = name_id_nq = name_id_spnq = None
        if 'samlNameId' in session:
            name_id = session['samlNameId']
        if 'samlSessionIndex' in session:
            session_index = session['samlSessionIndex']
        if 'samlNameIdFormat' in session:
            name_id_format = session['samlNameIdFormat']
        if 'samlNameIdNameQualifier' in session:
            name_id_nq = session['samlNameIdNameQualifier']
        if 'samlNameIdSPNameQualifier' in session:
            name_id_spnq = session['samlNameIdSPNameQualifier']
        session.clear()
        return redirect(auth.logout(name_id=name_id, session_index=session_index, nq=name_id_nq, name_id_format=name_id_format, spnq=name_id_spnq))
    elif 'acs' in request.args:
        request_id = None
        if 'AuthNRequestID' in session:
            request_id = session['AuthNRequestID']

        auth.process_response(request_id=request_id)
        errors = auth.get_errors()

        not_auth_warn = not auth.is_authenticated()
        if len(errors) == 0:
            if 'AuthNRequestID' in session:
                del session['AuthNRequestID']
            session['samlUserdata'] = auth.get_attributes()
            session['samlNameId'] = auth.get_nameid()
            session['samlNameIdFormat'] = auth.get_nameid_format()
            session['samlNameIdNameQualifier'] = auth.get_nameid_nq()
            session['samlNameIdSPNameQualifier'] = auth.get_nameid_spnq()
            session['samlSessionIndex'] = auth.get_session_index()
            session['login'] = True
            store_sso_data(session['samlUserdata'])

            logging.info('atributos del usuario: %s', session['samlUserdata'])
            self_url = OneLogin_Saml2_Utils.get_self_url(req)
            if 'RelayState' in request.form and self_url != request.form['RelayState']:
                # To avoid 'Open Redirect' attacks, before execute the redirection confirm
                # the value of the request.form['RelayState'] is a trusted URL.
                return redirect(auth.redirect_to(request.form['RelayState']))
            
        elif auth.get_settings().is_debug_active():
            error_reason = auth.get_last_error_reason()
            logging.info('Error: %s', error_reason)

    elif 'sls' in request.args:
        request_id = None
        if 'LogoutRequestID' in session:
            request_id = session['LogoutRequestID']

        def dscb(): return session.clear()
        url = auth.process_slo(request_id=request_id, delete_session_cb=dscb)
        errors = auth.get_errors()
        if len(errors) == 0:
            if url is not None:
                # To avoid 'Open Redirect' attacks, before execute the redirection confirm
                # the value of the url is a trusted URL.
                return redirect(url)
            else:
                success_slo = True
        elif auth.get_settings().is_debug_active():
            error_reason = auth.get_last_error_reason()

    if 'samlUserdata' in session:
        paint_logout = True
        if len(session['samlUserdata']) > 0:
            attributes = session['samlUserdata'].items()
    return redirect(redirect_url)


@api_bp.route('/auth/attrs/')
def attrs():
    paint_logout = False
    attributes = False

    if 'samlUserdata' in session:
        paint_logout = True
        if len(session['samlUserdata']) > 0:
            attributes = session['samlUserdata'].items()

    return render_template('attrs.html', paint_logout=paint_logout,
                           attributes=attributes)


@api_bp.route('/auth/metadata/')
def metadata():
    req = prepare_flask_request(request)
    auth = init_saml_auth(req)
    settings = auth.get_settings()
    metadata = settings.get_sp_metadata()
    errors = settings.validate_metadata(metadata)

    if len(errors) == 0:
        resp = make_response(metadata, 200)
        resp.headers['Content-Type'] = 'text/xml'
    else:
        resp = make_response(', '.join(errors), 500)
    return resp

app.register_blueprint(api_bp)

if __name__ == '__main__':
    app.run(port=8001)
