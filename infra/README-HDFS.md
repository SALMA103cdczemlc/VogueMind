# Hadoop HDFS — VogueMind

**Réalisée par :** Salma Hilali  
**Responsabilité :** Infrastructure Hadoop HDFS — Docker

## 1. Présentation

Dans le cadre du projet **VogueMind**, ma partie consiste à mettre en place une infrastructure de stockage distribué avec **Apache Hadoop HDFS**, en utilisant Docker Compose.

L'objectif est de disposer d'un espace de stockage fiable et persistant pour les données du projet, qui utilise également Apache Kafka et Apache Spark.

HDFS permet de stocker les fichiers sur plusieurs nœuds et de conserver plusieurs copies des blocs pour limiter les risques de perte de données.

## 2. Architecture HDFS

L'infrastructure Hadoop est composée de trois conteneurs Docker :

| Composant | Conteneur | Rôle |
|---|---|---|
| NameNode | `voguemind-namenode` | Gère les métadonnées et l'organisation des fichiers |
| DataNode 1 | `voguemind-datanode1` | Stocke les blocs de données |
| DataNode 2 | `voguemind-datanode2` | Stocke les blocs et participe à la réplication |

### Configuration technique

- **Version Hadoop :** 3.2.1
- **Architecture :** 1 NameNode et 2 DataNodes
- **Facteur de réplication :** 2
- **Réseau Docker :** `voguemind-net`
- **Stockage :** 3 volumes Docker persistants
- **Interface Web NameNode :** http://localhost:9870
- **Adresse HDFS interne :** `hdfs://namenode:9000`

Chaque DataNode possède son propre volume Docker pour conserver les données même après l'arrêt ou la recréation des conteneurs.

## 3. Déploiement du cluster

### Prérequis

Pour démarrer l'infrastructure, nous avons utilisé :

- Docker Desktop
- Docker Compose

Depuis la racine du projet, nous avons exécuté :

```bash
docker compose -f infra/docker-compose.hdfs.yml up -d
```

Pour vérifier l'état des conteneurs :

```bash
docker compose -f infra/docker-compose.hdfs.yml ps
```

Les trois conteneurs ont été démarrés avec succès et affichent l'état `healthy`.

## 4. Vérification du cluster

### 4.1. Vérification des DataNodes

Pour vérifier que les deux DataNodes sont connectés au NameNode, nous avons utilisé :

```bash
docker exec voguemind-namenode hdfs dfsadmin -report
```

**Résultat obtenu :**

```text
Live datanodes (2):
```

Ce résultat confirme que les deux DataNodes sont reconnus par le NameNode.

### 4.2. Vérification du facteur de réplication

Commande utilisée :

```bash
docker exec voguemind-namenode hdfs getconf -confKey dfs.replication
```

**Résultat obtenu :**

```text
2
```

Le facteur de réplication est donc configuré à 2. Cela signifie que HDFS cherche à conserver deux copies de chaque bloc de données.

## 5. Organisation du stockage HDFS

Pour organiser les données de VogueMind, nous avons créé quatre répertoires.

| Répertoire | Utilisation |
|---|---|
| `/voguemind/raw` | Données brutes |
| `/voguemind/processed` | Données traitées par Spark |
| `/voguemind/checkpoints` | Checkpoints des traitements streaming |
| `/voguemind/models` | Éventuels fichiers de modèles ou résultats analytiques |

### Création des répertoires

```bash
docker exec voguemind-namenode hdfs dfs -mkdir -p /voguemind/raw /voguemind/processed /voguemind/checkpoints /voguemind/models
```

### Vérification

```bash
docker exec voguemind-namenode hdfs dfs -ls /voguemind
```

**Résultat obtenu :**

```text
Found 4 items
/voguemind/checkpoints
/voguemind/models
/voguemind/processed
/voguemind/raw
```

Cette organisation permet de séparer les données selon leur utilisation dans le projet.

## 6. Tests de fonctionnement

Après le déploiement, plusieurs tests ont été réalisés pour vérifier que HDFS fonctionne correctement.

### 6.1. Test d'écriture et de lecture

Nous avons créé un fichier `test.txt`, puis nous l'avons transféré dans HDFS.

Pour vérifier son contenu :

```bash
docker exec voguemind-namenode hdfs dfs -cat /voguemind/raw/test.txt
```

**Résultat :**

```text
VogueMind HDFS test
```

Le fichier a été correctement enregistré et relu depuis HDFS.

### 6.2. Test de réplication

Pour vérifier la réplication du fichier :

```bash
docker exec voguemind-namenode hdfs fsck /voguemind/raw/test.txt -files -blocks -locations
```

**Résultats obtenus :**

```text
replication=2
Live_repl=2
Status: HEALTHY
Number of data-nodes: 2
Under-replicated blocks: 0
Missing blocks: 0
Corrupt blocks: 0
```

Ce test confirme que le fichier possède bien deux répliques et qu'aucun bloc n'est manquant ou corrompu.

### 6.3. Test de persistance

Nous avons également vérifié que les fichiers restent disponibles après l'arrêt et la recréation des conteneurs.

Commandes utilisées :

```bash
docker compose -f infra/docker-compose.hdfs.yml down
docker compose -f infra/docker-compose.hdfs.yml up -d
```

Après le redémarrage, le fichier de test était toujours accessible.

Ce résultat confirme que les volumes Docker permettent de conserver les données.

## 7. Intégration avec Kafka et Spark

L'infrastructure HDFS constitue la couche de stockage du pipeline Big Data VogueMind.

Chaque technologie possède un rôle :

- **Apache Kafka :** ingestion et transmission des données.
- **Apache Spark :** traitement et transformation des données.
- **Apache Hadoop HDFS :** stockage distribué des fichiers.

L'adresse utilisée pour accéder à HDFS depuis les conteneurs du réseau est :

```text
hdfs://namenode:9000
```

L'intégration entre Spark et HDFS a été testée avec succès. L'intégration complète du pipeline avec Kafka reste à finaliser.

## 8. Arrêt du cluster

Pour arrêter les conteneurs sans supprimer les volumes :

```bash
docker compose -f infra/docker-compose.hdfs.yml down
```

**Attention :** il ne faut pas utiliser `down -v` si nous souhaitons conserver les volumes Docker et les données enregistrées.

## 9. Captures d'écran et résultats

Cette partie présente les principales vérifications réalisées pendant la mise en place de l'infrastructure.

### 9.1. Déploiement du cluster Hadoop

Les trois conteneurs Hadoop ont été démarrés avec succès et affichent l'état `healthy`.

![Déploiement du cluster Hadoop](https://github.com/user-attachments/assets/b816b35f-7939-4614-b737-29c4f9b5cff3)

### 9.2. État du NameNode HDFS

L'interface Web du NameNode permet de consulter l'état général du cluster Hadoop.

Elle confirme que les deux DataNodes sont connectés et opérationnels.

![Interface NameNode HDFS](https://github.com/user-attachments/assets/6efe2349-9fde-4071-ad61-30a7a1eeb4ce)

### 9.3. Organisation des répertoires HDFS

Les quatre répertoires `raw`, `processed`, `checkpoints` et `models` ont été créés pour organiser le stockage du projet.

![Répertoires HDFS](https://github.com/user-attachments/assets/5afe36a5-4374-4673-bb01-74fbdaf9ef18)

### 9.4. Vérification de la réplication

La commande `hdfs fsck` a permis de vérifier l'état des blocs et leur réplication.

Le résultat `HEALTHY` confirme que le fichier testé ne présente aucun problème d'intégrité.

![Vérification de la réplication HDFS](https://github.com/user-attachments/assets/45780cba-585b-49be-99bf-68ad6f596813)

### 9.5. Importation du dataset H&M — Semaine 2

Pendant la deuxième semaine, nous avons commencé à travailler avec le dataset H&M utilisé dans le projet VogueMind.

Un échantillon de **1 000 articles** a été extrait du fichier `articles.csv`, puis importé dans HDFS.

**Caractéristiques du fichier :**

- **Nom :** `articles_sample.csv`
- **Nombre d'articles :** 1 000
- **Taille :** 355 997 octets
- **Chemin HDFS :** `/voguemind/raw/articles_sample.csv`

**Commandes utilisées :**

```bash
docker cp ./data/articles_sample.csv voguemind-namenode:/tmp/articles_sample.csv

docker exec voguemind-namenode hdfs dfs -put -f /tmp/articles_sample.csv /voguemind/raw/

docker exec voguemind-namenode hdfs dfs -cat /voguemind/raw/articles_sample.csv
```

Le fichier a été importé avec succès.

Nous avons pu consulter les informations des articles, notamment leurs identifiants, leurs noms, leurs catégories, leurs couleurs et leurs descriptions.

### 9.6. Vérification de l'intégrité du dataset H&M

Après l'importation, nous avons vérifié l'état du fichier avec la commande :

```bash
docker exec voguemind-namenode hdfs fsck /voguemind/raw/articles_sample.csv -files -blocks -locations
```

**Résultats obtenus :**

```text
Status: HEALTHY
Number of data-nodes: 2
Total size: 355997 B
Total files: 1
```

Le fichier est correctement stocké dans HDFS et aucun problème d'intégrité n'a été détecté.

Le cluster utilise un facteur de réplication de 2.

### 9.7. Vérification des deux DataNodes HDFS

Nous avons consulté l'onglet **Datanodes** de l'interface Hadoop afin de vérifier l'état des nœuds de stockage.

**Résultats observés :**

- 2 DataNodes en service.
- Les deux nœuds sont actifs.
- Capacité affichée : environ 1006,85 Go par DataNode.
- Version Hadoop : 3.2.1.

L'histogramme montre également que l'espace disque utilisé reste très faible.

Cette vérification confirme que les deux nœuds de stockage sont disponibles pour les traitements du projet.

**Capture : état des DataNodes HDFS**



### 9.8. Vérification du cluster Apache Spark

Nous avons ensuite consulté l'interface Web de **Spark Master** afin de vérifier que les workers étaient bien démarrés.

**Résultats obtenus :**

| Élément | Résultat |
|---|---|
| Version Spark | 3.5.3 |
| Spark Master | `spark://spark-master:7077` |
| Workers actifs | 2 |
| Cœurs disponibles | 2 |
| Mémoire totale | 2 Go |
| Statut | ALIVE |

Les deux workers possèdent chacun un cœur CPU et 1 Go de mémoire.

Au moment de la capture, aucune application Spark n'était en cours d'exécution.

Cette étape confirme que le cluster Spark est opérationnel et prêt à exécuter des traitements distribués.

**Capture : Spark Master avec les deux workers actifs**

<img width="1883" height="875" alt="image" src="https://github.com/user-attachments/assets/b4ebe0e6-b199-49b7-a3a4-03bac5077fa4" />


### 9.9. Test d'intégration Spark-HDFS

Pour terminer les vérifications, nous avons réalisé un test afin de confirmer que Spark peut écrire et lire des données dans Hadoop HDFS.

Le test a été effectué avec **10 000 lignes de données**, enregistrées au format Parquet.

**Chemin HDFS utilisé :**

```text
hdfs://namenode:9000/voguemind/tests/spark_hdfs_check
```

Après l'écriture, les données ont été relues pour vérifier leur intégrité.

**Résultats obtenus :**

```text
Écriture Parquet terminée
Facteur(s) de réplication observé(s) : [2]

--- Résultats ---
[OK] Relecture de 10000 lignes
[OK] Partitionnement par date (22 jours)
[OK] Schéma conservé
[OK] Réplication HDFS = 2
RESULTAT : intégration Spark-HDFS opérationnelle
```

Ce test a permis de confirmer que :

- Spark peut enregistrer des données dans HDFS.
- Les 10 000 lignes ont été relues correctement.
- Le partitionnement par date fonctionne sur 22 jours.
- Le schéma des données est conservé.
- Les blocs sont répliqués avec un facteur de 2.

Nous avons donc réussi à valider la communication entre Spark et HDFS.

**Capture : résultat du test Spark-HDFS**

<img width="915" height="390" alt="image" src="https://github.com/user-attachments/assets/407bae44-9e25-47d4-99f0-9165f0d84a59" />

### 9.10. Validation du pipeline Kafka → Spark → HDFS

Nous avons réalisé un test d'intégration avec 100 transactions du dataset H&M afin de vérifier leur transfert depuis Apache Kafka vers Hadoop HDFS à l'aide d'Apache Spark Structured Streaming.

**Résultats obtenus :**

```text
[batch 0] 100 transactions archivées, 0 rejetées
Job terminé.
```

Ce résultat confirme que Spark a traité les transactions reçues depuis Kafka et les a archivées dans HDFS au format Parquet.

<img width="876" height="282" alt="image" src="https://github.com/user-attachments/assets/b7036212-645e-4af2-a281-cc5df9469468" />


### 9.11. Vérification de la chaîne Producer → Kafka → Spark

Un second test a permis de vérifier la transmission des transactions entre le producteur Python, Apache Kafka et Apache Spark.

**Résultats obtenus :**

```text
Messages invalides (JSON non conforme) : 0
OK chaîne producer.py -> Kafka -> Spark opérationnelle
```

Le test confirme que les messages analysés sont conformes au format JSON attendu et que la communication entre les trois composants fonctionne correctement.

<img width="617" height="97" alt="image" src="https://github.com/user-attachments/assets/384a7196-3251-47b9-902f-0ce597f8159b" />
### 9.12. Test automatique de validation des données HDFS

Pour compléter les tests d'intégration du projet VogueMind, j'ai développé un script Python permettant de vérifier automatiquement les transactions archivées dans Hadoop HDFS après leur traitement par Apache Spark.

**Fichier réalisé :** `tests/integration/test_pipeline_end_to_end.py`

Ce test vérifie :
- La présence des transactions archivées dans HDFS.
- La conformité du schéma des données.
- La qualité des transactions enregistrées.
- La présence des fichiers au format Parquet.
- Le facteur de réplication HDFS égal à 2.

**Commande d'exécution :**

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/tests/integration/test_pipeline_end_to_end.py
```

**Résultats obtenus :**

```text
=== VOGUEMIND - VALIDATION HDFS ===
Transactions archivees : 100
Fichiers Parquet : 3
[PASS] Transactions archivees
[PASS] Schema attendu
[PASS] Qualite des transactions
[PASS] Fichiers Parquet presents
[PASS] Replication HDFS = 2
RESULTAT : SUCCES
```

Tous les contrôles ont réussi.

Ce test confirme que les 100 transactions déjà traitées par le pipeline sont accessibles dans HDFS, que leur structure est conforme aux attentes et que les trois fichiers Parquet possèdent un facteur de réplication de 2.

Le script constitue un livrable technique supplémentaire permettant de contrôler la sortie HDFS du pipeline, sans modifier les données existantes.

**Capture d'écran : résultat du test automatique**
<img width="571" height="296" alt="image" src="https://github.com/user-attachments/assets/fa4d5bea-1319-438d-a180-07e2e076deae" />

### 9.13. Organisation et archivage de l'historique des transactions

Cette semaine, nous avons poursuivi le traitement des transactions du dataset H&M avec Apache Spark.

Les transactions traitées ont été archivées dans Hadoop HDFS afin de conserver un historique exploitable pour les analyses et les traitements ultérieurs.

Le stockage est organisé dans le répertoire :

```text
/voguemind/processed/history/transactions
```

Les données sont organisées par date à l'aide de partitions, ce qui facilite leur consultation et leur traitement.

La vérification du répertoire HDFS a permis de constater la présence de **85 éléments**, comprenant les partitions de données et le fichier `_SUCCESS`.

Cette étape permet de conserver un historique structuré des transactions et de préparer les données pour les analyses métier et les expérimentations de machine learning.

### 9.14. Calcul des indicateurs métier avec Apache Spark

Nous avons également développé et exécuté un script Python nommé `indicators.py` pour calculer des indicateurs à partir des données archivées dans HDFS.

Ce traitement utilise Apache Spark et Spark SQL afin d'exploiter les transactions stockées dans le système distribué.

Les résultats sont enregistrés dans le répertoire :

```text
/voguemind/processed/indicators
```

Cette étape permet de transformer les données transactionnelles en informations synthétiques utiles à l'analyse du comportement des clients et au suivi de l'activité.

Le calcul des indicateurs constitue une étape intermédiaire entre le stockage des données brutes ou traitées et leur exploitation dans les fonctionnalités analytiques de VogueMind.

### 9.15. Entraînement d'un modèle de prédiction de réachat

Cette semaine, nous avons également commencé la partie Machine Learning du projet en développant le script :

```text
batch/train_model.py
```

L'objectif est de prédire la probabilité qu'un client effectue un nouvel achat, à partir de son historique de transactions.

Pour cette première expérimentation, nous avons utilisé un échantillon correspondant à **5 % des clients**, afin de réaliser les tests dans l'environnement Spark disponible.

Le traitement a été exécuté avec la commande :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/voguemind/batch/train_model.py --customer-pct 5 --version v1
```

**Données utilisées :**

| Élément | Nombre |
|---|---:|
| Données d'entraînement | 76 852 |
| Données de validation | 13 201 |
| Données de test | 13 048 |

La séparation des données a été réalisée chronologiquement afin de respecter l'ordre temporel des transactions.

**Modèles expérimentés :**

- Une règle de référence basée sur un achat effectué dans les 7 jours.
- Logistic Regression.
- Random Forest.
- Gradient Boosting.

Après la comparaison des modèles sur les données de validation, la régression logistique a été retenue pour une étape de réglage des hyperparamètres.

Les meilleurs paramètres obtenus pendant cette étape sont :

- `regParam = 0.01`
- `elasticNetParam = 0.5`

### 9.16. Évaluation du modèle sur les données de test

Après le réglage du modèle, nous avons évalué ses performances sur un ensemble de test distinct.

Les résultats obtenus sont les suivants :

| Métrique | Modèle de référence | Régression logistique |
|---|---:|---:|
| Precision | 0.188 | 0.207 |
| Recall | 0.374 | 0.458 |
| F1-score | 0.250 | 0.285 |
| AUC | — | 0.632 |

Les résultats montrent une amélioration du F1-score, qui passe de **0.250 à 0.285**, ainsi qu'une amélioration du rappel.

Le modèle permet donc d'obtenir de meilleurs résultats que la règle de référence sur cet échantillon de test. Toutefois, les performances restent perfectibles et nécessitent des expérimentations supplémentaires.

Cette première évaluation constitue une base pour améliorer progressivement la prédiction du réachat dans VogueMind.

### 9.17. Sauvegarde du modèle entraîné dans HDFS

Après l'entraînement et l'évaluation, nous avons sauvegardé le modèle de régression logistique dans Hadoop HDFS.

Le modèle est enregistré dans le répertoire suivant :

```text
/voguemind/models/repurchase_v1
```

Son adresse complète est :

```text
hdfs://namenode:9000/voguemind/models/repurchase_v1
```

La vérification du contenu du répertoire a permis de constater la présence des différents éléments du pipeline MLlib, notamment :

- StringIndexer.
- OneHotEncoder.
- VectorAssembler.
- StandardScaler.
- LogisticRegression.

Les fichiers de données et les métadonnées du modèle sont également présents dans HDFS.

Cette sauvegarde permet de conserver le modèle entraîné dans l'infrastructure distribuée du projet et de préparer son utilisation dans une prochaine étape de prédiction.

### 9.18. Génération des rapports de résultats du Machine Learning

Afin de conserver les résultats de l'expérimentation, nous avons généré deux fichiers récapitulatifs :

```text
results/ml_metrics_v1.md
results/ml_metrics_v1.json
```

Le premier fichier présente les résultats sous forme de rapport lisible, tandis que le second conserve les métriques au format JSON pour faciliter leur exploitation ultérieure.

Ces fichiers permettent de documenter les expérimentations et de comparer les performances des futures versions du modèle.

### 9.19. Sauvegarde des développements sur GitHub

À la fin de cette étape, nous avons également sauvegardé les nouveaux développements et les résultats dans le dépôt GitHub du projet VogueMind.

Les éléments ajoutés comprennent notamment :

- Les captures et éléments de documentation HDFS déjà réalisés.
- Le script `inspect_raw.py`.
- Les rapports de résultats du Machine Learning.
- Les modifications apportées à la configuration Docker Compose.

Les modifications ont été enregistrées dans un commit intitulé :

```text
Add HDFS screenshots and ML training results
```

Le commit a ensuite été envoyé avec succès sur la branche `main` du dépôt GitHub.

Cette étape permet de conserver une version centralisée des développements réalisés et de faciliter le suivi du travail de l'équipe.

## 10. Conclusion

Cette partie du projet VogueMind m'a permis de participer à la mise en place et à l'évolution de l'infrastructure Big Data basée sur Docker, Apache Hadoop HDFS et Apache Spark.

J'ai commencé par configurer le NameNode et les deux DataNodes, puis j'ai vérifié le fonctionnement du cluster, la réplication des fichiers et la persistance des données.

Nous avons ensuite importé un échantillon du dataset H&M dans HDFS et validé son intégrité. L'intégration entre Spark et HDFS a également été testée à travers des opérations d'écriture et de lecture de données au format Parquet.

Au cours de cette semaine, le travail a été poursuivi avec l'archivage des transactions, le calcul des indicateurs métier et une première expérimentation de Machine Learning visant à prédire le réachat des clients.

Un modèle de régression logistique a été entraîné, évalué et sauvegardé dans HDFS. Les résultats ont montré une amélioration par rapport à la règle de référence, même si des améliorations restent nécessaires.

Enfin, les nouveaux développements et les rapports de résultats ont été sauvegardés sur GitHub.

L'infrastructure HDFS est opérationnelle, les échanges entre Kafka, Spark et HDFS ont été testés sur un échantillon de transactions, et une première version du modèle de prédiction de réachat a été entraînée et enregistrée.

Les prochaines étapes consistent à améliorer les performances du modèle, à exploiter celui-ci pour générer des prédictions clients et à poursuivre son intégration dans le système de recommandation de VogueMind.

---

**Projet :** VogueMind  
**Composant :** Infrastructure Hadoop HDFS et intégration Big Data  
**Réalisée par :** Salma Hilali  
**Statut :** Cluster HDFS opérationnel, intégration Kafka–Spark–HDFS testée, indicateurs calculés et première version du modèle de prédiction de réachat entraînée et sauvegardée dans HDFS.
