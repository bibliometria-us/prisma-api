def procesado_fichero():
    # TODO: Implementar procesado del fichero con todas las funciones secundarias que requiera

    # 1. Transformar a csv (esto hay que verlo, podemos forzar a que el fichero se suba como csv, o admitir xls/xlsx y si fuese, transformarlo a csv)
    #
    # 2. Normalizar columnas. Si acabamos con ficheros con columnas con nombres distintos, gestionarlo para que se puedan leer.
    # La idea es que siempre tengamos un fichero final con la misma forma sin importar el input.
    #
    # 3. Decidir el nombre de fichero, con un formato tipo YYYYMMDDmmss.csv
    #
    # 4. Almacenarlo en temp/carga_centros_censo/<filename>
    #
    # 5. Devolver la ruta del fichero para pasarsela al AsyncRequest como parámetro
    #
    # NOTA: En los pasos de normalización/reconstrucción del fichero, aprovechar para hacer todas las comprobaciones posibles. Por ejemplo,
    # si hay alguna columna mal, un DNI en mal formato/que falte... Y levantar excepciones si se detecta algo, para que se lancen antes de iniciar el proceso.

    pass
