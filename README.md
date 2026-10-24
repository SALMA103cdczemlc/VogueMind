# VogueMind

## Présentation
VogueMind est un projet Big Data de recommandation personnalisée de vêtements utilisant le dataset H&M Personalized Fashion Recommendations.

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

infra/
  kafka-compose.yml

notebook/
  FashionIQ.ipynb
```

## Exécution de la partie Kafka

Installer les dépendances :

```bash
python -m pip install pandas kafka-python
```

Démarrer Kafka :

```bash
docker compose -f infra/kafka-compose.yml up -d
```

Lancer le producteur :

```bash
python ingestion/producer.py
```

Lancer le consommateur :

```bash
python ingestion/consumer.py
```

## Nada Rian — Apache Spark et intégration

Travail réalisé :
- Cluster Spark standalone : 1 Master et 2 Workers (image Spark 3.5.3 avec connecteur Kafka intégré).
- `docker-compose.yml` commun à la racine : HDFS + Kafka + Spark sur le réseau `voguemind-net`.
- Deux listeners Kafka : `localhost:9092` pour la machine hôte, `kafka:29092` pour les conteneurs Spark.
- Tests de communication entre conteneurs et tests d'intégration Spark → HDFS et Kafka → Spark → HDFS.

Documentation détaillée : [infra/README-SPARK.md](infra/README-SPARK.md)

## Lancement du pipeline complet (compose commun)

```bash
cp .env.example .env                  # optionnel
docker compose up -d --build          # HDFS + Kafka + Spark
docker compose ps                     # attendre l'état "healthy"
./scripts/verify_integration.sh       # tests d'intégration
```

| Interface | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Spark Workers | http://localhost:8081 · http://localhost:8082 |
| HDFS NameNode | http://localhost:9870 |

Le producteur et le consommateur Python fonctionnent sans modification avec ce compose (`localhost:9092`).

## Dataset
Source : H&M Personalized Fashion Recommendations sur Kaggle.

Les fichiers CSV doivent être placés localement dans le dossier `data/`. Ils ne sont pas inclus dans le dépôt GitHub en raison de leur taille.

## Équipe
- Safae Benhaddou : exploration des données et ingestion Kafka.
- Salma Hilali : stockage HDFS.
- Nada Rian : traitement Spark et recommandations.
