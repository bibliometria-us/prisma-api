from flask import Flask, Response
from flask_saml import saml2

app = Flask(__name__)
# URL where metadata will be available
app.config['SAML_METADATA_URL'] = '/saml/metadata'

saml = saml2()


@app.route('/saml/metadata')
def saml_metadata():
    metadata_xml = saml.metadata()
    response = Response(metadata_xml, content_type='text/xml')
    return response


if __name__ == '__main__':
    app.run()
