from db.conexion import BaseDatos

bd = BaseDatos()
query_jcr = """SELECT MAX(year) FROM m_jcr"""
bd.ejecutarConsulta(query_jcr)

max_jcr_year = bd.get_first_cell()


bd = BaseDatos()
query_sjr = """SELECT MAX(year) FROM m_sjr"""
bd.ejecutarConsulta(query_sjr)

max_sjr_year = bd.get_first_cell()


bd = BaseDatos()
query_idr = """SELECT MAX(anualidad) FROM m_idr"""
bd.ejecutarConsulta(query_idr)

max_idr_year = bd.get_first_cell()


bd = BaseDatos()
query_jci = """SELECT MAX(agno) FROM m_jci"""
bd.ejecutarConsulta(query_jci)

max_jci_year = bd.get_first_cell()
