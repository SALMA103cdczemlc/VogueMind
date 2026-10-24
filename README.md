# VogueMind

## Présentation
FashionIQ est un projet Big Data de recommandation personnalisée de vêtements utilisant le dataset H&M Personalized Fashion Recommendations.

## Technologies
- Python et Pandas
- Apache Kafka
- Docker
- Apache Spark
- Hadoop HDFS
- Machine Learning

## Architecture du projet
Dataset H&M → Python Producer → Apache Kafka → Traitement et stockage Big Data → Recommandations personnalisées

## Safae Benhaddou — Ingestion des données

Travail réalisé :
- Téléchargement et exploration des données H&M.
- Configuration d'Apache Kafka avec Docker.
- Création du topic `fashion-transactions`.
- Développement du producteur Python `producer.py`.
- Développement du consommateur Python `consumer.py`.
- Test réussi avec 1 000 transactions H&M.

## Structure des fichiers

```text
ingestion/
  producer.py
  consumer.py

infrastructure/
  kafka-compose.yml

notebooks/
  exploration_hm.ipynb
```

## Exécution de la partie Kafka

Installer les dépendances :

```bash
python -m pip install pandas kafka-python
```

Démarrer Kafka :

```bash
docker compose -f infrastructure/kafka-compose.yml up -d
```

Lancer le producteur :

```bash
python ingestion/producer.py
```

Lancer le consommateur :

```bash
python ingestion/consumer.py
```

## Dataset
Source : H&M Personalized Fashion Recommendations sur Kaggle.

Les fichiers CSV doivent être placés localement dans le dossier `data/`. Ils ne sont pas inclus dans le dépôt GitHub en raison de leur taille.

## Équipe
- Safae Benhaddou : exploration des données et ingestion Kafka.
- Salma Hilali : stockage HDFS.
- Nada Rian : traitement Spark et recommandations.
