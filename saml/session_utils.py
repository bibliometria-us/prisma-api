from flask import session


def get_email_from_session(tipo: str = "us"):
    if not session:
        return None

    mails: list[str] = session["samlUserdata"]["mail"]
    if tipo == "us":
        endswith = "@us.es"

    return next((mail for mail in mails if mail.endswith(endswith)), None)

def get_user_from_session(tipo: str = "us"):
    email = get_email_from_session(tipo=tipo)
    if not email:
        return
    
    user = email.split("@")[0]

    return user