class ExcepcionJournalWoS(Exception):
    pass

class RevistaWosNoEncontrada(ExcepcionJournalWoS):
    def __init__(self, id_fuente, issns):
        self.message = f"Revista {id_fuente} con ISSN{'s' if len(issns) > 1 else ''} {issns} no encontrada en WoS"
    
    def __str__(self):
        return self.message
    
class ISSNsNoEncontrados(ExcepcionJournalWoS):
    def __init__(self, id_fuente):
        self.message = f"No existe ISSN para la revista con id en Prisma {id_fuente}"
    
    def __str__(self):
        return self.message
    
class MetricasNoEncontradas(ExcepcionJournalWoS):
    def __init__(self, id_fuente, id_wos, año):
        self.message = f"No existen métricas para la revista {id_wos} ({id_fuente}) en el año {año}"
    
    def __str__(self):
        return self.message
    
class MetricaNoEncontrada(ExcepcionJournalWoS):
    def __init__(self, id_fuente, id_wos, año, tipo, categoria = None):
        self.message = f"No existe métrica {tipo} para la revista {id_wos} ({id_fuente}){f' y categoría {categoria} ' if categoria else ''}en el año {año}"
    
    def __str__(self):
        return self.message
    
class ErrorCargaMetrica(ExcepcionJournalWoS):
    def __init__(self, id_fuente, id_wos,  año, tipo):
        self.message = f"Error cargando la métrica {tipo} para la revista {id_wos} ({id_fuente}) en el año {año}"
    
    def __str__(self):
        return self.message