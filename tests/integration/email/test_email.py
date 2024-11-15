from integration.email.email import enviar_correo


def test_email():
    enviar_correo(
        destinatarios="ppadasdasdzo@dasdasdadfus.es",
        texto_plano="prueba",
        texto_html="",
        asunto="prueba",
    )
