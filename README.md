# Segmentation des clients e-commerce avec Apache Spark et Kafka

Un projet Big Data complet qui effectue une segmentation par lots des clients en utilisant l'analyse RFM (Récence, Fréquence, Montant), compare des algorithmes de clustering et démontre l'enrichissement de transactions en temps réel à l'aide d'Apache Kafka et de Spark Structured Streaming.

## Table des matières

- [Aperçu du projet](#aperçu-du-projet)
- [Flux de travail](#flux-de-travail)
- [Résultats et Analyse](#résultats-et-analyse)
- [Jeu de données](#jeu-de-données)
- [Prérequis](#prérequis)
- [Prise en main](#prise-en-main)
- [Guide d'exécution](#guide-dexécution)
- [Structure du dépôt](#structure-du-dépôt)
- [Notes et limitations](#notes-et-limitations)
- [Technologies](#technologies)

## Aperçu du projet

Ce dépôt met en œuvre un pipeline de données de bout en bout conçu pour segmenter les clients d'un site e-commerce et appliquer ces segments à des données en flux continu. L'architecture est divisée en deux phases principales :

- **Traitement par lots (Batch Processing) :** Utilise Apache Spark et les DataFrames PySpark pour nettoyer les données, concevoir les caractéristiques RFM et entraîner des modèles de clustering à l'aide de Spark MLlib.
- **Évaluation du modèle :** Compare les algorithmes K-Means et de Clustering Hiérarchique (liaison de Ward) en utilisant le score de silhouette avec une distance euclidienne au carré afin de sélectionner le modèle optimal.
- **Traitement de flux (Stream Processing) :** Utilise Apache Kafka et Spark Structured Streaming pour consommer des événements de transaction JSON en direct et les enrichir avec les segments clients précalculés.
- **Environnement :** Entièrement conteneurisé à l'aide de Docker Compose pour une exécution reproductible, avec une interface Jupyter Notebook.

## Flux de travail

1. **Ingestion des données :** Charger et explorer le jeu de données "Online Retail".
2. **Nettoyage des données :** Supprimer les enregistrements en double, les lignes sans identifiant client, les factures annulées et les transactions avec des quantités ou des prix non positifs.
3. **Ingénierie des caractéristiques :** Calculer le revenu par transaction et agréger les caractéristiques RFM au niveau du client.
4. **Prétraitement :** Appliquer des transformations logarithmiques et normaliser (standardiser) les caractéristiques RFM pour assurer une mise à l'échelle uniforme.
5. **Sélection du modèle :** Évaluer le clustering K-Means pour des valeurs de K allant de 2 à 8.
6. **Comparaison des algorithmes :** Comparer le modèle K-Means optimal avec le Clustering Hiérarchique (liaison de Ward) en utilisant deux clusters et des vecteurs de caractéristiques normalisés.
7. **Sérialisation :** Sélectionner l'algorithme avec le score de silhouette le plus élevé, analyser les profils clients résultants et sauvegarder la sortie finale du modèle de segmentation au format Parquet.
8. **Enrichissement du flux :** Consommer des transactions JSON à partir d'un sujet (topic) Kafka et les joindre avec la segmentation sauvegardée pour attribuer des étiquettes de cluster en temps réel.

*Remarque : Le pipeline de streaming utilise la segmentation précalculée et n'entraîne pas à nouveau le modèle. Les clients absents de la segmentation sauvegardée se verront attribuer un cluster nul (null).*

## Résultats et Analyse

Le jeu de données original "Online Retail" contient 541 909 lignes. Après l'application du pipeline de prétraitement, le jeu de données est réduit à environ 392 692 transactions valides, représentant 4 338 clients uniques, 18 532 factures et 3 665 produits uniques.

### Sélection du modèle K-Means

L'implémentation K-Means de Spark MLlib a été évaluée pour des valeurs de K allant de 2 à 8 en utilisant le `ClusteringEvaluator` avec `distanceMeasure="squaredEuclidean"`.

| K | Score de silhouette | Coût d'entraînement |
|---|---------------------|---------------------|
| 2 | 0.6238              | 6486.51             |
| 3 | 0.5013              | 4878.00             |
| 4 | 0.4999              | 3961.85             |
| 5 | 0.4772              | 3330.25             |
| 6 | 0.4704              | 2874.82             |
| 7 | 0.4379              | 2595.93             |
| 8 | 0.4398              | 2361.80             |

Parmi les valeurs testées, **K = 2** obtient le score de silhouette le plus élevé.

### Comparaison des algorithmes de clustering

K-Means et le Clustering Hiérarchique (liaison de Ward) ont été comparés en utilisant les mêmes vecteurs de caractéristiques RFM normalisés et deux clusters. L'évaluation a utilisé le score de silhouette de scikit-learn avec `metric="sqeuclidean"`.

| Algorithme                       | Clusters | Score de silhouette |
|----------------------------------|----------|---------------------|
| K-Means                          | 2        | 0.6238              |
| Clustering Hiérarchique (Ward)   | 2        | 0.5567              |

K-Means a obtenu le score de silhouette le plus élevé et a été sélectionné pour la segmentation finale sauvegardée. Les deux étapes d'évaluation utilisent la distance euclidienne au carré ; le score de sélection du modèle K-Means est calculé par Spark MLlib, tandis que les scores de comparaison des algorithmes sont calculés avec scikit-learn.

### Profils clients

La segmentation K-Means sélectionnée produit les profils descriptifs suivants :

| Cluster | Clients | Part   | Récence moy. (jours) | Fréquence moy. (factures) | Valeur monétaire moy. |
|---------|---------|--------|----------------------|---------------------------|-----------------------|
| 0       | 1 654   | 38.13% | 25.39                | 8.49                      | 4 566.18              |
| 1       | 2 684   | 61.87% | 133.15               | 1.68                      | 497.30                |

Le Cluster 0 représente les clients à forte valeur, avec des achats récents et un engagement élevé. Le Cluster 1 représente les clients à plus faible valeur, avec des achats moins fréquents et moins récents. Il s'agit de profils descriptifs déduits des données, et non d'étiquettes commerciales prédéfinies.

## Jeu de données

Le projet utilise le jeu de données **Online Retail**.

- **Source principale :** [UCI Machine Learning Repository - Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail)
- **Miroir alternatif :** [erkansirin78/datasets - OnlineRetail.csv](https://github.com/erkansirin78/datasets/blob/master/OnlineRetail.csv)

Le jeu de données est exclu du contrôle de version en raison de sa taille. Téléchargez-le dans `project/data/raw/OnlineRetail.csv` à l'aide de la commande suivante :

```bash
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv
