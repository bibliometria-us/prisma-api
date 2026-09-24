from pydantic import BaseModel


class Investigador(BaseModel):
    id: int
    nombre: str
    apellido: str
    email: str
    telefono: str
    fecha_nacimiento: str
    genero: str
    nacionalidad: str
    institucion: str
    departamento: str
    cargo: str
