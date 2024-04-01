import email.utils
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from integration.email.config import correo
import time


ruta_log = ""


def salida(texto):
    if ruta_log != "":
        fichero = open(ruta_log, "a+")
        fichero.write("{0}\n".format(texto))
        fichero.close()
    try:
        print(texto)
    except UnicodeEncodeError:
        print(texto.encode("UTF-8"))


def enviar_correo(destinatarios, texto_plano, texto_html, asunto, adjuntos=None):
    """
    Función para enviar correos
    @param destinatarios: lista con los emails de los destinatarios
    @param texto_plano: texto plano del mensaje (alternativa al html)
    @param texto_html: texto formateado en html (altenativa al texto plano)
    @param asunto: asunto del email
    @param adjuntos: array con los ficheros que se van a adjuntar al correo. Por cada fichero habrá una entrada 'ruta'
    con la ruta al fichero y otra 'nombre' con el nombre con el que se enviará el fichero adjunto
    @:return bool Si ha envidado el correo o no
    """
    enviado = False
    if adjuntos is None:
        adjuntos = []
    msg_alt = MIMEMultipart("alternative")
    pie1 = "--\nBiblioteca de la Universidad de Sevilla\nUnidad de Bibliometría\nhttps://bibliometria.us.es"
    pie2 = (
        "<table style='font-family:Roboto; padding-top: 15px;'><tr>"
        "<td width='360' valign='top'>"
        "<img src='https://bibliometria.us.es/comun/images/logoBibliometriaCorreo.png' "
        "alt='Biblioteca de la Universidad de Sevilla - Unidad de Bibliometria'/></td></tr></table>"
        "<table style='font-family:Roboto; width: 360px; background-color: #f3f3f3; margin-top: 15px; padding: "
        "3px 3px 5px 3px; border-spacing: 5px; text-align: center;'><tr><td width='215' valign='top'>"
        "<a href='https://bibliometria.us.es' target='_blank' style='margin: 0 auto; text-decoration: none; "
        "font-size: 16px;'>"
        "<span style='color: #000; text-decoration: underline;'>bibliometria.us.es</span></a></td> </tr></table>"
        "<table style='font-family:Roboto; padding-top: 15px;'><tr>"
        "<td width='360' valign='top'>"
        "<img src='https://sic.us.es/sites/default/files/servicios/correo/Archivos/us_logo.jpg' "
        "alt='Universidad de Sevilla'/></td></tr></table>"
        "<table style='font-family:Roboto; width: 360px; background-color: #f3f3f3; margin-top: 15px; "
        "padding: 3px 3px 5px 3px; border-spacing: 5px; text-align: center;'><tr><td width='215' valign='top'>"
        "<a href='http://www.us.es/' target='_blank' style='margin: 0 auto; text-decoration: none; "
        "font-size: 16px;'>"
        "<span style='color: #000; text-decoration: underline;'>www.us.es</span></a></td> </tr></table>"
        "<table style='font-family:Roboto; width: 360px; font-size: 9px; padding-top: 20px; border-spacing: 5px;'>"
        "<tr><td width='215' valign='top'> <span style='color: #339966;'>Por favor considere el medio ambiente "
        "antes de imprimir este correo electr&oacute;nico. </span><br>-<br><span style='color: #999999;'>Este "
        "correo electr&oacute;nico y, en su caso, cualquier fichero anexo al mismo, contiene informaci&oacute;n "
        "de car&aacute;cter confidencial exclusivamente dirigida a su destinatario o destinatarios. Si no es Vd. "
        "el destinatario del mensaje, le ruego lo destruya sin hacer copia digital o f&iacute;sica, comunicando "
        "al emisor por esta misma v&iacute;a la recepci&oacute;n del presente mensaje. Gracias"
        "</span></td></tr></table>"
    )

    html = texto_html + pie2
    texto = texto_plano + pie1

    msg_alt.attach(MIMEText(texto, "plain"))
    msg_alt.attach(MIMEText(html, "html"))

    msg = MIMEMultipart("mixed")
    msg["To"] = ", ".join(destinatarios)
    msg["From"] = email.utils.formataddr(
        (
            "Unidad de Bibliometria - Biblioteca de la Universidad de Sevilla",
            "bibliometria@us.es",
        )
    )
    msg["Subject"] = asunto
    msg.attach(msg_alt)

    for fichero in adjuntos:
        nombre_fichero = fichero.split("/")[-1]
        mime = "octet-stream"
        if "mime" in fichero:
            mime = fichero["mime"]
        adjunto = MIMEBase("application", mime)
        print(fichero)
        adjunto.set_payload((open(fichero, "rb")).read())
        encoders.encode_base64(adjunto)
        adjunto.add_header(
            "Content-Disposition",
            "attachment",
            filename=("utf-8", "es", nombre_fichero),
        )
        msg.attach(adjunto)

    server = smtplib.SMTP(correo["servidor"], port=correo["puerto"])
    try:
        server.starttls()
        server.login(correo["usuario"], correo["clave"])
        # server.login(Claves.correo['usuario'], Claves.correo['claves'])
        server.sendmail("bibliometria@us.es", destinatarios, msg.as_string())
        txt_destinatarios = ",".join(destinatarios)
        salida(
            "{}: Correo envíado a {}".format(
                time.strftime("%d/%m/%y %H:%M"), txt_destinatarios
            )
        )
        enviado = True
    except Exception as e:
        salida("{}: Error al enviar el correo".format(time.strftime("%d/%m/%y %H:%M")))
        print(e)
        enviado = False
    finally:
        server.quit()
        return enviado
