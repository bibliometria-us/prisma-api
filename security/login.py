from flask import session


def is_logged_in():
    return session.get("login", False)
