from pandas import DataFrame
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from thefuzz import fuzz
import math


class ComparacionAutores:
    def __init__(
        self,
        nuevos_autores: DataFrame,
        antiguos_autores: DataFrame,
    ) -> None:
        self.nuevos_autores = nuevos_autores
        self.antiguos_autores = antiguos_autores
        self.set_firma_autores = set()
        self.vector = None
        self.k = 1
        self.clusters: dict[int, list[str]] = {}
        self.reversed_name_cluster: dict[str, int] = {}

    def comparar(self, tipo_comparacion="cantidad"):
        """
        Decidir el tipo de comparación que se va a utilizar.
        """
        if tipo_comparacion == "cantidad":
            return self.comparar_cantidad_autores()
        if tipo_comparacion == "fuzz_ratio":
            self.comparar_fuzz_ratio()

    def comparar_cantidad_autores(self):
        """
        Realizar una comparación solo por la longitud de las listas de autores de la publicación.
        """

        count_nuevos = self.nuevos_autores["tipo"].value_counts().to_dict()
        count_antiguos = self.antiguos_autores["tipo"].value_counts().to_dict()

        if count_nuevos != count_antiguos:
            return count_nuevos, count_antiguos
        else:
            return None

    def comparar_fuzz_ratio(self):
        """
        Este método compara la similitud de autores mediante fuzz ratio, permitiendo detectar coincidencias
        en firmas de autor con ligeras diferencias por uso de siglas o typos.
        """
        self.set_firma_autores = set(
            self.nuevos_autores["firma"].tolist()
            + self.antiguos_autores["firma"].tolist()
        )

        self.vectorizar()
        self.k_means()
        self.add_cluster_numbers()
        self.comparar_nuevos_autores()
        pass

    def vectorizar(self):
        vectorizer = TfidfVectorizer(analyzer="char", ngram_range=(3, 4))
        X = vectorizer.fit_transform(self.set_firma_autores)
        self.vector = X

    def calcular_k(self):
        self.k = int(math.sqrt(len(self.set_firma_autores)))

    def k_means(self):
        self.calcular_k()
        kmeans = KMeans(n_clusters=self.k, random_state=42)

        kmeans.fit(self.vector)
        labels = kmeans.labels_

        clusters = {}

        for name, label in zip(self.set_firma_autores, labels):
            clusters.setdefault(label, []).append(name)

        reversed_cluster = {
            name: key for key, names in clusters.items() for name in names
        }

        self.clusters = clusters
        self.reversed_name_cluster = reversed_cluster

    def add_cluster_numbers(self):
        for nombre, index in self.reversed_name_cluster.items():

            self.nuevos_autores.loc[
                self.nuevos_autores["firma"] == nombre, "cluster"
            ] = index
            self.nuevos_autores["cluster"] = (
                pd.to_numeric(self.nuevos_autores["cluster"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

            self.antiguos_autores.loc[
                self.antiguos_autores["firma"] == nombre, "cluster"
            ] = index
            self.antiguos_autores["cluster"] = (
                pd.to_numeric(self.antiguos_autores["cluster"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

    def comparar_nuevos_autores(self):
        for index, autor in self.nuevos_autores.iterrows():
            antiguos_autores = self.antiguos_autores[
                self.antiguos_autores["cluster"] == autor["cluster"]
            ]

            autor_similar, max_fuzz_ratio = max(
                (
                    (
                        antiguo_autor,
                        fuzz.ratio(autor["firma"], antiguo_autor.firma),
                    )
                    for antiguo_autor in antiguos_autores.itertuples()
                ),
                key=lambda item: item[
                    1
                ],  # Compare based on the fuzz.ratio value (item[1])
            )
