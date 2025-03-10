from integration.email.email import enviar_correo


def test_email():
    enviar_correo(
        destinatarios="ppazo@us.es",
        texto_plano="prueba",
        texto_html="",
        asunto="prueba",
    )
