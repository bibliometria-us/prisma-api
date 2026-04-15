import sys
import os

# Añadir el directorio raíz al path
sys.path.insert(0, "/app")
from db.conexion import BaseDatos
from collections import defaultdict


class ConsolidadorFuentesDuplicadas:
    """Clase para consolidar fuentes duplicadas por ISSN/EISSN"""

    def __init__(self, auto_commit=False):
        self.db = BaseDatos(keep_connection_alive=True, autocommit=False)
        self.auto_commit = auto_commit
        self.consolidaciones_realizadas = []

    def obtener_fuentes_duplicadas(self):
        """
        Identifica grupos de fuentes con el mismo ISSN/EISSN

        Returns:
            dict: Grupos de fuentes duplicadas organizados por (tipo, valor)
        """
        query = """
            SELECT 
                pif.tipo,
                pif.valor,
                pf.idFuente,
                pf.titulo,
                COUNT(pp.idPublicacion) as num_publicaciones
            FROM p_fuente pf
            INNER JOIN p_identificador_fuente pif ON pif.idFuente = pf.idFuente
            LEFT JOIN p_publicacion pp ON pp.idFuente = pf.idFuente 
                AND pp.eliminado != 1
            WHERE pf.eliminado != 1
            AND pif.tipo IN ('issn', 'eissn')
            GROUP BY pif.tipo, pif.valor, pf.idFuente, pf.titulo
            ORDER BY pif.tipo, pif.valor, num_publicaciones DESC, pf.idFuente ASC
        """

        self.db.ejecutarConsulta(query)
        resultados = self.db.get_dataframe()

        # Agrupar por tipo+valor para identificar duplicados
        grupos = defaultdict(list)
        total_fuentes = 0
        total_publicaciones = 0

        for _, row in resultados.iterrows():
            key = (row["tipo"], row["valor"])
            grupos[key].append(
                {
                    "idFuente": row["idFuente"],
                    "titulo": row["titulo"],
                    "num_publicaciones": row["num_publicaciones"],
                }
            )
            total_fuentes += 1
            total_publicaciones += row["num_publicaciones"]

        # Filtrar solo los que tienen duplicados
        duplicados = {k: v for k, v in grupos.items() if len(v) > 1}

        # Calcular estadísticas
        fuentes_duplicadas = sum(len(v) for v in duplicados.values())
        fuentes_unicas = total_fuentes - fuentes_duplicadas + len(duplicados)
        publicaciones_duplicadas = sum(
            sum(f["num_publicaciones"] for f in fuentes)
            for fuentes in duplicados.values()
        )

        # Mostrar resumen
        print(f"\n{'='*80}")
        print("RESUMEN DE ANÁLISIS")
        print(f"{'='*80}")
        print(f"Total de fuentes analizadas:        {total_fuentes}")
        print(f"Fuentes únicas (sin duplicar):      {fuentes_unicas}")
        print(f"Fuentes duplicadas:                 {fuentes_duplicadas}")
        print(f"Grupos de duplicados encontrados:   {len(duplicados)}")
        print(f"Total de publicaciones afectadas:   {publicaciones_duplicadas}")
        print(f"{'='*80}")

        return duplicados

    def consolidar_grupo(self, tipo, valor, fuentes, id_mantener):
        """
        Consolida un grupo específico de fuentes duplicadas

        Args:
            tipo: Tipo de identificador ('issn' o 'eissn')
            valor: Valor del identificador
            fuentes: Lista de fuentes del grupo
            id_mantener: ID de la fuente a mantener

        Returns:
            bool: True si se consolidó correctamente, False si hubo error
        """
        try:
            titulo_mantener = next(
                f["titulo"] for f in fuentes if f["idFuente"] == id_mantener
            )
            fuentes_a_eliminar = [f for f in fuentes if f["idFuente"] != id_mantener]

            print(f"\n{'='*60}")
            print(f"Consolidando {tipo.upper()}: {valor}")
            print(f"Manteniendo: {id_mantener} - {titulo_mantener[:50]}...")
            print(f"{'='*60}")

            total_publicaciones_movidas = 0

            for fuente in fuentes_a_eliminar:
                params = {"mantener": id_mantener, "eliminar": fuente["idFuente"]}

                # Actualizar publicaciones
                query_update_pub = """
                    UPDATE p_publicacion 
                    SET idFuente = %(mantener)s 
                    WHERE idFuente = %(eliminar)s
                    AND eliminado != 1
                """
                self.db.ejecutarConsulta(query_update_pub, params)
                filas = self.db.rowcount
                total_publicaciones_movidas += filas
                print(
                    f"  ✓ {filas} publicaciones movidas de {fuente['idFuente']} ({fuente['titulo'][:40]}...)"
                )

                # Eliminar datos de la fuente
                query_eliminar_datos = """
                    DELETE FROM p_dato_fuente 
                    WHERE idFuente = %(eliminar)s
                """
                self.db.ejecutarConsulta(
                    query_eliminar_datos, {"eliminar": fuente["idFuente"]}
                )
                filas_datos = self.db.rowcount
                print(
                    f"  ✓ {filas_datos} datos eliminados de la fuente {fuente['idFuente']}"
                )

                # Eliminar identificadores de la fuente
                query_eliminar_ident = """
                    DELETE FROM p_identificador_fuente 
                    WHERE idFuente = %(eliminar)s
                """
                self.db.ejecutarConsulta(
                    query_eliminar_ident, {"eliminar": fuente["idFuente"]}
                )
                filas_ident = self.db.rowcount
                print(
                    f"  ✓ {filas_ident} identificadores eliminados de la fuente {fuente['idFuente']}"
                )

                # Marcar fuente como eliminada
                query_eliminar_fuente = """
                    UPDATE p_fuente 
                    SET eliminado = 1 
                    WHERE idFuente = %(eliminar)s
                """
                self.db.ejecutarConsulta(
                    query_eliminar_fuente, {"eliminar": fuente["idFuente"]}
                )
                print(f"  ✓ Fuente {fuente['idFuente']} marcada como eliminada")

            # Commit inmediato
            self.db.commit()

            print(
                f"\n✅ Consolidación completada: {total_publicaciones_movidas} publicaciones movidas en total"
            )

            # Guardar registro de la consolidación
            self.consolidaciones_realizadas.append(
                {
                    "tipo": tipo,
                    "valor": valor,
                    "mantener": id_mantener,
                    "eliminadas": [f["idFuente"] for f in fuentes_a_eliminar],
                    "publicaciones_movidas": total_publicaciones_movidas,
                }
            )

            return True

        except Exception as e:
            print(f"\n❌ Error durante la consolidación: {e}")
            self.db.rollback()
            print("⚠️  Cambios revertidos para este grupo")
            return False

    def procesar_duplicados(self, duplicados):
        """
        Procesa cada grupo de duplicados pidiendo confirmación

        Args:
            duplicados: dict con grupos de fuentes duplicadas
        """
        total_grupos = len(duplicados)
        grupo_actual = 0

        for (tipo, valor), fuentes in duplicados.items():
            grupo_actual += 1

            print(f"\n{'='*80}")
            print(f"GRUPO {grupo_actual}/{total_grupos}")
            print(f"Tipo: {tipo.upper()} | Valor: {valor}")
            print(f"{'='*80}")

            for i, fuente in enumerate(fuentes):
                marcador = " ← MANTENER (más publicaciones)" if i == 0 else ""
                print(
                    f"{i+1}. ID: {fuente['idFuente']} | {fuente['titulo']} | "
                    f"Publicaciones: {fuente['num_publicaciones']}{marcador}"
                )

            # La primera es la que se mantiene por defecto
            mantener = fuentes[0]["idFuente"]

            print(f"\n→ Por defecto se mantendrá la fuente {mantener}")
            confirmacion = input(
                "¿Proceder con esta consolidación? (s/n/cambiar/salir): "
            ).lower()

            if confirmacion == "s":
                self.consolidar_grupo(tipo, valor, fuentes, mantener)

            elif confirmacion == "cambiar":
                try:
                    nuevo_mantener = int(
                        input("Introduce el ID de la fuente a mantener: ")
                    )
                    if nuevo_mantener in [f["idFuente"] for f in fuentes]:
                        self.consolidar_grupo(tipo, valor, fuentes, nuevo_mantener)
                    else:
                        print("❌ ID no válido para este grupo. Saltando...")
                except ValueError:
                    print("❌ ID inválido. Saltando este grupo...")

            elif confirmacion == "salir":
                print("\n⊘ Saliendo del proceso...")
                break

            else:
                print("⊘ Saltando este grupo...")

    def mostrar_resumen_final(self):
        """Muestra un resumen final de todas las consolidaciones realizadas"""
        if not self.consolidaciones_realizadas:
            print("\n⊘ No se realizaron consolidaciones.")
            return

        print(f"\n\n{'='*80}")
        print("RESUMEN FINAL DE CONSOLIDACIONES")
        print(f"{'='*80}")
        print(f"Total de grupos consolidados: {len(self.consolidaciones_realizadas)}")

        total_eliminadas = sum(
            len(c["eliminadas"]) for c in self.consolidaciones_realizadas
        )
        total_pubs_movidas = sum(
            c["publicaciones_movidas"] for c in self.consolidaciones_realizadas
        )

        print(f"Total de fuentes eliminadas: {total_eliminadas}")
        print(f"Total de publicaciones movidas: {total_pubs_movidas}")
        print(f"{'='*80}")

        print("\nDetalle por grupo:")
        for i, c in enumerate(self.consolidaciones_realizadas, 1):
            print(f"\n{i}. {c['tipo'].upper()}: {c['valor']}")
            print(f"   Mantenida: {c['mantener']}")
            print(f"   Eliminadas: {', '.join(map(str, c['eliminadas']))}")
            print(f"   Publicaciones movidas: {c['publicaciones_movidas']}")

    def close_database(self):
        """Cierra la conexión con la base de datos"""
        self.db.closeConnection()


def main():
    """Función principal para ejecutar la consolidación"""
    print("=" * 80)
    print("CONSOLIDADOR DE FUENTES DUPLICADAS")
    print("=" * 80)

    consolidador = ConsolidadorFuentesDuplicadas()

    try:
        # 1. Obtener duplicados y mostrar resumen
        duplicados = consolidador.obtener_fuentes_duplicadas()

        if not duplicados:
            print("\n✓ No se encontraron fuentes duplicadas")
            consolidador.close_database()
            return

        # 2. Procesar cada grupo (consolida inmediatamente al confirmar)
        consolidador.procesar_duplicados(duplicados)

        # 3. Mostrar resumen final
        consolidador.mostrar_resumen_final()

    except KeyboardInterrupt:
        print("\n\n⊘ Operación cancelada por el usuario")
    except Exception as e:
        print(f"\n✗ Error inesperado: {e}")
        raise
    finally:
        consolidador.close_database()


if __name__ == "__main__":
    main()
