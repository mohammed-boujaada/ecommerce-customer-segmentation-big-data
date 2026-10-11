# Segmentation Client E-commerce avec Apache Spark et Kafka

Projet Big Data visant à segmenter une base de clients e-commerce via une analyse RFM (Récence, Fréquence, Montant). Ce projet compare l'efficacité de deux algorithmes de clustering et met en œuvre un pipeline de traitement de flux en temps réel (Spark Structured Streaming et Apache Kafka) pour l'enrichissement dynamique des transactions.

Développé par : Mohammed Boujaada et Mohamed Zahir

## Aperçu du projet

- Traitement Big Data : Apache Spark, PySpark DataFrames et opérations RDD.
- Analyse client : Ingénierie des fonctionnalités basées sur le modèle RFM.
- Machine Learning : Transformation logarithmique, standardisation, et clustering (K-Means et Clustering Hiérarchique) avec Spark MLlib.
- Comparaison de modèles : Évaluation comparative via le score de silhouette sur des vecteurs de caractéristiques standardisés.
- Traitement de flux : Enrichissement en temps réel des événements de transaction Kafka via Spark Structured Streaming.
- Environnement d'exécution : Docker Compose et Jupyter Notebook.

## Flux de traitement (Workflow)

1. Chargement et exploration du jeu de données "Online Retail".
2. Nettoyage des données : suppression des doublons, des entrées sans identifiant client, des factures annulées et des transactions aux quantités ou prix non positifs.
3. Ingénierie des fonctionnalités : calcul du revenu par transaction et agrégation des métriques RFM au niveau client.
4. Prétraitement : application de transformations logarithmiques et standardisation des caractéristiques RFM.
5. Évaluation du modèle K-Means pour des valeurs de K comprises entre 2 et 8.
6. Comparaison des performances entre K-Means et le Clustering Hiérarchique (liaison de Ward) pour K=2, en utilisant le score de silhouette.
7. Sélection de l'algorithme optimal, analyse des profils clients générés et sauvegarde de la segmentation finale au format Parquet.
8. Consommation de flux de transactions JSON via Kafka et association des clients reconnus à leur segment précalculé.

Le notebook de streaming utilise la segmentation sauvegardée et ne réentraîne pas le modèle. Les clients non reconnus se voient attribuer une valeur nulle pour le cluster.

## Résultats et Analyses

Le jeu de données initial contient 541 909 lignes. Après l'application du pipeline de nettoyage, le projet traite environ 392 692 transactions valides, réparties entre 4 338 clients, 18 532 factures et 3 665 produits.

### Sélection du modèle K-Means
Évaluation des valeurs de K de 2 à 8 avec l'évaluateur de clustering de Spark (distance euclidienne au carré).

| K | Score de silhouette (Spark) |
|---|-----------------------------|
| 2 | 0.6238                      |
| 3 | 0.5013                      |
| 4 | 0.4999                      |
| 5 | 0.4772                      |
| 6 | 0.4704                      |
| 7 | 0.4379                      |
| 8 | 0.4398                      |

La valeur K = 2 présente le score de silhouette le plus élevé.

### Comparaison des algorithmes
Comparaison entre K-Means et le Clustering Hiérarchique (Ward) sur 2 clusters, évaluée avec le score de silhouette de scikit-learn sur les mêmes vecteurs standardisés.

| Algorithme                      | Clusters | Score de silhouette |
|---------------------------------|----------|---------------------|
| K-Means                         | 2        | 0.4340              |
| Clustering Hiérarchique (Ward)  | 2        | 0.3933              |

Le modèle K-Means est retenu pour ses performances supérieures dans cette configuration.

### Profils clients identifiés
La solution K-Means retenue génère les profils descriptifs suivants :

| Cluster | Clients | Part    | Récence moyenne (jours) | Fréquence moyenne (factures) | Valeur monétaire moyenne |
|---------|---------|---------|-------------------------|------------------------------|--------------------------|
| 0       | 1 654   | 38.13%  | 25.39                   | 8.49                         | 4 566.18                 |
| 1       | 2 684   | 61.87%  | 133.15                  | 1.68                         | 497.30                   |

Le Cluster 0 représente les clients à forte valeur, avec des achats récents et fréquents. Le Cluster 1 regroupe les clients à faible valeur, avec des achats moins récents et moins fréquents.

## Jeu de données

Le projet utilise le jeu de données **Online Retail**.
- Source principale : [UCI Machine Learning Repository - Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail)
- Téléchargement alternatif : [erkansirin78/datasets - OnlineRetail.csv](https://github.com/erkansirin78/datasets/blob/master/OnlineRetail.csv)

Le jeu de données n'est pas inclus dans ce dépôt pour des raisons de taille. Il doit être téléchargé manuellement dans le répertoire `project/data/raw/OnlineRetail.csv` :

```bash
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv