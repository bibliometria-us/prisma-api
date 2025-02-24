import pandas as pd

from db.conexion import BaseDatos


def leer_csv() -> list[str]:
    path = "routes/informes/acuerdos_transformativos/fuente/dois.csv"
    with open(path, "r") as f:
        df = pd.read_csv(f)

    return df["DOI"].tolist()


def informe_autores_por_publicaciones():
    dois = leer_csv()

    bd = BaseDatos()
    query_autores = f"""SELECT 
            doi.valor as 'DOI',
            CONCAT(i.apellidos, ", ", i.nombre) as 'Nombre',
            i.idInvestigador as 'ID Prisma',
            c.nombre as 'Categoria',
            CASE WHEN i.sexo = 1 THEN "Hombre" ELSE "Mujer" END as 'Género',
            d.nombre as 'Departamento',
            a.nombre as 'Área',
            b.nombre as 'Biblioteca',
            centro.nombre as 'Centro'
            FROM p_publicacion p
            LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = "doi") doi ON doi.idPublicacion = p.idPublicacion
            LEFT JOIN (SELECT * FROM p_autor WHERE idInvestigador != 0) autor ON autor.idPublicacion = p.idPublicacion
            LEFT JOIN i_investigador i ON i.idInvestigador = autor.idInvestigador
            LEFT JOIN i_categoria c ON c.idCategoria = i.idCategoria
            LEFT JOIN i_departamento d ON d.idDepartamento = i.idDepartamento
            LEFT JOIN i_area a ON a.idArea = i.idArea
            LEFT JOIN investigador_biblioteca ib ON ib.idInvestigador = i.idInvestigador
            LEFT JOIN i_biblioteca b ON b.nombre = ib.biblioteca
            LEFT JOIN i_centro centro ON centro.idBiblioteca = b.idBiblioteca
            WHERE doi.valor IN ({", ".join(f"'{doi}'" for doi in dois)})
            GROUP BY doi.valor, i.idInvestigador
            ORDER BY doi.valor, i.idInvestigador;
            """
    autores = bd.ejecutarConsulta(query_autores)
    df_autores = bd.get_dataframe()

    missing_dois = set(dois) - set(df_autores["DOI"])
    missing_rows = pd.DataFrame({"DOI": list(missing_dois)})
    df_autores = pd.concat([df_autores, missing_rows], ignore_index=True)

    query_jif = f"""SELECT 
            doi.valor AS 'DOI',
            f.titulo AS 'Revista',
            j.edition as 'Edición',
            j.category as 'Categoría',
            j.impact_factor as 'JIF',
            j.quartile as 'Cuartil',
            (SELECT MIN(j2.quartile) FROM m_jcr j2 WHERE j2.year = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Cuartil',
            j.decil as 'Decil',
            (SELECT MIN(j2.decil) FROM m_jcr j2 WHERE j2.year = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Decil',
            j.tercil as 'Tercil',
            (SELECT MIN(j2.tercil) FROM m_jcr j2 WHERE j2.year = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Tercil'
            FROM p_publicacion p
            LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = "doi") doi ON doi.idPublicacion = p.idPublicacion
            LEFT JOIN p_fuente f ON f.idFuente = p.idFuente
            LEFT JOIN (SELECT * FROM m_jcr WHERE year = '2023') j ON j.idFuente = f.idFuente
            WHERE doi.valor IN ({", ".join(f"'{doi}'" for doi in dois)})
            GROUP BY doi.valor, j.idFuente, j.edition, j.category
            ORDER BY doi.valor, j.idFuente;
            """
    jif = bd.ejecutarConsulta(query_jif)
    df_jif = bd.get_dataframe()

    missing_dois = set(dois) - set(df_jif["DOI"])
    missing_rows = pd.DataFrame({"DOI": list(missing_dois)})
    df_jif = pd.concat([df_jif, missing_rows], ignore_index=True)

    query_jci = f"""SELECT 
            doi.valor AS 'DOI',
            f.titulo AS 'Revista',
            j.categoria as 'Categoría',
            j.jci as 'JCI',
            j.cuartil as 'Cuartil',
            (SELECT MIN(j2.cuartil) FROM m_jci j2 WHERE j2.agno = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Cuartil',
            j.decil as 'Decil',
            (SELECT MIN(j2.decil) FROM m_jci j2 WHERE j2.agno = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Decil',
            j.tercil as 'Tercil',
            (SELECT MIN(j2.tercil) FROM m_jci j2 WHERE j2.agno = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Tercil'
            FROM p_publicacion p
            LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = "doi") doi ON doi.idPublicacion = p.idPublicacion
            LEFT JOIN p_fuente f ON f.idFuente = p.idFuente
            LEFT JOIN (SELECT * FROM m_jci WHERE agno = '2023') j ON j.idFuente = f.idFuente
            WHERE doi.valor IN ({", ".join(f"'{doi}'" for doi in dois)})
            GROUP BY doi.valor, j.idFuente, j.categoria
            ORDER BY doi.valor, j.idFuente;
            """
    jci = bd.ejecutarConsulta(query_jci)
    df_jci = bd.get_dataframe()

    missing_dois = set(dois) - set(df_jci["DOI"])
    missing_rows = pd.DataFrame({"DOI": list(missing_dois)})
    df_jci = pd.concat([df_jci, missing_rows], ignore_index=True)

    query_citescore = f"""SELECT 
            doi.valor AS 'DOI',
            f.titulo AS 'Revista',
            j.categoria as 'Categoría',
            j.citescore as 'Citescore',
            j.cuartil as 'Cuartil',
            (SELECT MIN(j2.cuartil) FROM m_citescore j2 WHERE j2.agno = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Cuartil',
            j.decil as 'Decil',
            (SELECT MIN(j2.decil) FROM m_citescore j2 WHERE j2.agno = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Decil',
            j.tercil as 'Tercil',
            (SELECT MIN(j2.tercil) FROM m_citescore j2 WHERE j2.agno = '2023' AND j2.idFuente = j.idFuente) as 'Máximo Tercil'
            FROM p_publicacion p
            LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = "doi") doi ON doi.idPublicacion = p.idPublicacion
            LEFT JOIN p_fuente f ON f.idFuente = p.idFuente
            LEFT JOIN (SELECT * FROM m_citescore WHERE agno = '2023') j ON j.idFuente = f.idFuente
            WHERE doi.valor IN ({", ".join(f"'{doi}'" for doi in dois)})
            GROUP BY doi.valor, j.idFuente, j.categoria
            ORDER BY doi.valor, j.idFuente;
            """
    citescore = bd.ejecutarConsulta(query_citescore)
    df_citescore = bd.get_dataframe()

    missing_dois = set(dois) - set(df_citescore["DOI"])
    missing_rows = pd.DataFrame({"DOI": list(missing_dois)})
    df_citescore = pd.concat([df_citescore, missing_rows], ignore_index=True)

    with pd.ExcelWriter("informe_autores.xlsx", engine="xlsxwriter") as writer:
        df_autores.to_excel(writer, sheet_name="Autores", index=False)
        df_jif.to_excel(writer, sheet_name="JIF", index=False)
        df_jci.to_excel(writer, sheet_name="JCI", index=False)
        df_citescore.to_excel(writer, sheet_name="Citescore", index=False)
    pass
