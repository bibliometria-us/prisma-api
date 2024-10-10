

from routes.carga.publicacion.idus.parser import IdusParser


handles = [
    "11441/82033",
    "11441/41044",
    "11441/142092",
    "11441/47752",
]

def test_carga_idus():
    for handle in handles:
        parser = IdusParser(handle=handle)
        pass
        