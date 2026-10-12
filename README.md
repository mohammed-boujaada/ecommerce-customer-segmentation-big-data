
# Segmentation des clients e-commerce avec Apache Spark et Kafka

![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apache-spark&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apache-kafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)

Un projet Big Data complet qui effectue une segmentation par lots des clients en utilisant l'analyse RFM (Récence, Fréquence, Montant), compare des algorithmes de clustering et démontre l'enrichissement de transactions en temps réel à l'aide d'Apache Kafka et de Spark Structured Streaming.

## Table des matières

1. [Aperçu du projet](#1-aperçu-du-projet)
2. [Flux de travail](#2-flux-de-travail)
3. [Résultats et analyse](#3-résultats-et-analyse)
4. [Jeu de données](#4-jeu-de-données)
5. [Prérequis](#5-prérequis)
6. [Prise en main](#6-prise-en-main)
7. [Guide d'exécution](#7-guide-dexécution)
8. [Structure du dépôt](#8-structure-du-dépôt)
9. [Notes et limitations](#9-notes-et-limitations)
10. [Technologies](#10-technologies)
11. [Auteur](#11-auteur)

## 1. Aperçu du projet

Ce dépôt met en œuvre un pipeline de données de bout en bout conçu pour segmenter les clients d'un site e-commerce et appliquer ces segments à des données en flux continu. L'architecture est divisée en deux phases principales :

- **Traitement par lots (Batch Processing)** : utilise Apache Spark et les DataFrames PySpark pour nettoyer les données, concevoir les features RFM et entraîner des modèles de clustering à l'aide de Spark MLlib.
- **Évaluation du modèle** : compare les algorithmes K-Means et de Clustering Hiérarchique (liaison de Ward) en utilisant le score de silhouette avec une distance euclidienne au carré afin de sélectionner le modèle optimal.
- **Traitement de flux (Stream Processing)** : utilise Apache Kafka et Spark Structured Streaming pour consommer des événements de transaction JSON en direct et les enrichir avec les segments clients précalculés.

**Environnement** : entièrement conteneurisé à l'aide de Docker Compose pour une exécution reproductible, avec une interface Jupyter Notebook.

## 2. Flux de travail

1. **Ingestion des données** : charger et explorer le jeu de données *Online Retail*.
2. **Nettoyage des données** : supprimer les enregistrements en double, les lignes sans identifiant client, les factures annulées et les transactions avec des quantités ou des prix non positifs.
3. **Ingénierie des features** : calculer le revenu par transaction et agréger les features RFM au niveau du client.
4. **Prétraitement** : appliquer des transformations logarithmiques et normaliser (standardiser) les features RFM pour assurer une mise à l'échelle uniforme.
5. **Sélection du modèle** : évaluer le clustering K-Means pour des valeurs de K allant de 2 à 8.
6. **Comparaison des algorithmes** : comparer le modèle K-Means optimal avec le Clustering Hiérarchique (liaison de Ward) en utilisant deux clusters et des vecteurs de features normalisés.
7. **Sérialisation** : sélectionner l'algorithme avec le score de silhouette le plus élevé, analyser les profils clients résultants et sauvegarder la sortie finale du modèle de segmentation au format Parquet.
8. **Enrichissement du flux** : consommer des transactions JSON à partir d'un sujet (topic) Kafka et les joindre avec la segmentation sauvegardée pour attribuer des étiquettes de cluster en temps réel.

> **Remarque** : le pipeline de streaming utilise la segmentation précalculée et n'entraîne pas à nouveau le modèle. Les clients absents de la segmentation sauvegardée se verront attribuer un cluster nul (`null`).

## 3. Résultats et analyse

Le jeu de données original *Online Retail* contient 541 909 lignes. Après l'application du pipeline de prétraitement, le jeu de données est réduit à 392 692 transactions valides, représentant 4 338 clients uniques, 18 532 factures et 3 665 produits uniques.

### 3.1. Sélection du modèle K-Means

L'implémentation K-Means de Spark MLlib a été évaluée pour des valeurs de K allant de 2 à 8 en utilisant le `ClusteringEvaluator` avec `distanceMeasure="squaredEuclidean"`.

| K | Score de silhouette | Coût d'entraînement |
|---|---|---|
| 2 | **0.6238** | 6486.51 |
| 3 | 0.5013 | 4878.00 |
| 4 | 0.4999 | 3961.85 |
| 5 | 0.4772 | 3330.25 |
| 6 | 0.4704 | 2874.82 |
| 7 | 0.4379 | 2595.93 |
| 8 | 0.4398 | 2361.80 |

Parmi les valeurs testées, **K = 2** obtient le score de silhouette le plus élevé. Un score supérieur à 0.5 indique une structure de clusters raisonnable et un partitionnement pertinent des données.

### 3.2. Comparaison des algorithmes de clustering

K-Means et le Clustering Hiérarchique (liaison de Ward) ont été comparés en utilisant les mêmes vecteurs de features RFM normalisés et deux clusters. L'évaluation a utilisé le score de silhouette de scikit-learn avec `metric="sqeuclidean"`.

| Algorithme | Clusters | Score de silhouette |
|---|---|---|
| **K-Means** | 2 | **0.6238** |
| Clustering Hiérarchique (Ward) | 2 | 0.5567 |

K-Means a obtenu le score de silhouette le plus élevé et a été sélectionné pour la segmentation finale sauvegardée. Les deux étapes d'évaluation utilisent la distance euclidienne au carré ; le score de sélection du modèle K-Means est calculé par Spark MLlib, tandis que les scores de comparaison des algorithmes sont calculés avec scikit-learn.

### 3.3. Profils clients

La segmentation K-Means sélectionnée produit les profils descriptifs suivants :

| Cluster | Clients | Part | Récence moy. (jours) | Fréquence moy. (factures) | Valeur monétaire moy. |
|---|---|---|---|---|---|
| 0 | 1 654 | 38.13 % | 25.39 | 8.49 | 4 566.18 |
| 1 | 2 684 | 61.87 % | 133.15 | 1.68 | 497.30 |

- **Cluster 0** : clients à forte valeur, avec des achats récents et un engagement élevé.
- **Cluster 1** : clients à plus faible valeur, avec des achats moins fréquents et moins récents.

Il s'agit de profils descriptifs déduits des données, et non d'étiquettes commerciales prédéfinies.

## 4. Jeu de données

Le projet utilise le jeu de données *Online Retail*.

- **Source principale** : [UCI Machine Learning Repository - Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)
- **Miroir alternatif** : [erkansirin78/datasets - OnlineRetail.csv](https://github.com/erkansirin78/datasets)

Le jeu de données est exclu du contrôle de version en raison de sa taille. Téléchargez-le dans `project/data/raw/OnlineRetail.csv` à l'aide de la commande suivante :

```bash
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv
```

## 5. Prérequis

- **Docker** et **Docker Compose** installés et fonctionnels (le cluster Spark, Kafka et Jupyter sont fournis par les conteneurs).
- Au moins **4 Go de RAM** alloués à Docker (8 Go recommandés pour l'exécution simultanée de Spark et Kafka).
- `curl` ou un navigateur pour télécharger le jeu de données.
- Aucune installation locale de Spark, Kafka ou Java n'est requise.

## 6. Prise en main

```bash
# 1. Cloner le dépôt
git clone <URL_DU_DEPOT>
cd <nom-du-depot>

# 2. Télécharger le jeu de données (voir section 4)
mkdir -p project/data/raw
curl -L "https://raw.githubusercontent.com/erkansirin78/datasets/master/OnlineRetail.csv" -o project/data/raw/OnlineRetail.csv

# 3. Démarrer l'environnement conteneurisé
docker-compose up -d

# 4. Vérifier que les services sont démarrés
docker-compose ps
```

L'interface Jupyter Notebook est accessible à l'adresse : `http://localhost:8888`.

## 7. Guide d'exécution

Les notebooks se trouvent dans le dossier `project/notebooks/`.

1. **Phase batch** : ouvrir le notebook de segmentation dans Jupyter et exécuter les cellules dans l'ordre (nettoyage → features RFM → prétraitement → K-Means (K = 2 à 8) → comparaison avec Ward → analyse des profils → sauvegarde Parquet).
2. **Phase streaming** : démarrer le producteur de transactions (événements JSON vers le topic Kafka), puis exécuter le notebook de streaming qui consomme le flux et applique la jointure avec la segmentation sauvegardée.
3. **Arrêt de l'environnement** :

```bash
docker-compose down
```

## 8. Structure du dépôt

```text
.
├── docker-compose.yml           # Définition des services (Spark, Kafka, Jupyter)
├── project/
│   └── notebooks/               # Notebooks batch et streaming
├── images/                      # Captures d'écran et illustrations du README
├── .gitignore                   # Exclusion du jeu de données et des artefacts
└── README.md                    # Documentation du projet
```

Remarques :

- Le dossier `project/data/raw/` n'est pas versionné : il est créé à l'exécution par la commande de téléchargement (section 4) et exclu du contrôle de version via `.gitignore`.
- La sortie du modèle de segmentation (format Parquet) est également générée à l'exécution et exclue du contrôle de version.

## 9. Notes et limitations

- **Segmentation précalculée** : le streaming n'entraîne pas le modèle ; il applique une jointure avec la sortie batch. Les clients inconnus reçoivent un cluster `null`.
- **Choix de K = 2** : statistiquement optimal selon le score de silhouette, mais une segmentation en 3 ou 4 groupes pourrait offrir une granularité plus exploitable d'un point de vue métier.
- **Évaluation mixte** : les scores de silhouette proviennent de deux implémentations (Spark MLlib et scikit-learn) ; bien que la métrique soit identique (`squaredEuclidean`), de légères différences de calcul sont possibles.
- **Périmètre RFM** : la segmentation repose uniquement sur la Récence, la Fréquence et le Montant ; l'intégration de features produits (panier, catégories) constituerait une extension naturelle.
- **Jeu de données historique** : *Online Retail* couvre la période 2010-2011 ; les patterns comportementaux ne reflètent pas le e-commerce actuel.

## 10. Technologies

| Technologie | Utilisation |
|---|---|
| Apache Spark / PySpark | Nettoyage, feature engineering RFM, clustering batch |
| Spark MLlib | K-Means, ClusteringEvaluator |
| scikit-learn | Score de silhouette comparatif (Ward vs K-Means) |
| Apache Kafka | Ingestion des événements de transaction JSON |
| Spark Structured Streaming | Enrichissement temps réel du flux |
| Parquet | Sérialisation de la segmentation finale |
| Docker Compose | Conteneurisation et reproductibilité de l'environnement |
| Jupyter Notebook | Développement et exécution interactive |

## 11. Auteur

**Mohammed Boujaada**

Étudiant en Master Intelligence Artificielle et Technologies Émergentes  
Faculté des Sciences Appliquées de Nador (FSAN), Université Mohammed Premier, Maroc

- LinkedIn : https://www.linkedin.com/in/mohammed-boujaada-229a3333a/
- GitHub : https://github.com/mohammed-boujaada
