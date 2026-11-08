# Hadoop HDFS — VogueMind

**Réalisée par :** Salma Hilali
**Responsabilité :** Infrastructure Hadoop HDFS — Docker

## 1. Présentation

Dans le cadre du projet **VogueMind**, cette partie consiste à mettre en place une infrastructure de stockage distribué avec **Apache Hadoop HDFS**, déployée à l'aide de Docker Compose.

L'objectif est de disposer d'un environnement de stockage fiable, répliqué et persistant, destiné à accueillir les données du pipeline Big Data intégrant Apache Kafka et Apache Spark.

## 2. Architecture HDFS

L'infrastructure repose sur un cluster Hadoop composé de trois conteneurs Docker :

| Composant  | Conteneur             | Rôle                                                      |
| ---------- | --------------------- | --------------------------------------------------------- |
| NameNode   | `voguemind-namenode`  | Gestion des métadonnées et de l'organisation des fichiers |
| DataNode 1 | `voguemind-datanode1` | Stockage des blocs de données                             |
| DataNode 2 | `voguemind-datanode2` | Stockage des blocs et réplication                         |

### Configuration technique

- **Version Hadoop :** 3.2.1
- **Architecture :** 1 NameNode et 2 DataNodes
- **Facteur de réplication :** 2
- **Réseau Docker :** `voguemind-net`
- **Stockage :** 3 volumes Docker persistants et indépendants
- **Interface Web NameNode :** `http://localhost:9870`
- **Adresse HDFS interne :** `hdfs://namenode:9000`

Chaque DataNode dispose de son propre volume Docker, permettant de conserver les données indépendamment du cycle de vie des conteneurs.

## 3. Déploiement du cluster

### Prérequis

- Docker Desktop
- Docker Compose

Depuis la racine du projet, exécuter :

```bash
docker compose -f infra/docker-compose.hdfs.yml up -d
```

Vérifier ensuite l'état des conteneurs :

```bash
docker compose -f infra/docker-compose.hdfs.yml ps
```

Les trois conteneurs doivent être démarrés et afficher l'état `healthy`.

## 4. Vérification du cluster

### Vérification des DataNodes

```bash
docker exec voguemind-namenode hdfs dfsadmin -report
```

Résultat obtenu :

```text
Live datanodes (2):
```

Les deux DataNodes sont correctement enregistrés auprès du NameNode.

### Vérification du facteur de réplication

```bash
docker exec voguemind-namenode hdfs getconf -confKey dfs.replication
```

Résultat obtenu :

```text
2
```

Le cluster est configuré pour stocker deux copies de chaque bloc de données par défaut.

## 5. Organisation du stockage HDFS

Quatre répertoires ont été créés pour organiser les données du projet.

| Répertoire               | Utilisation prévue                                     |
| ------------------------ | ------------------------------------------------------ |
| `/voguemind/raw`         | Stockage des données brutes                            |
| `/voguemind/processed`   | Stockage des données transformées par Spark            |
| `/voguemind/checkpoints` | Sauvegarde des checkpoints des traitements streaming   |
| `/voguemind/models`      | Répertoire réservé à d'éventuels artefacts analytiques |

### Création des répertoires

```bash
docker exec voguemind-namenode hdfs dfs -mkdir -p /voguemind/raw /voguemind/processed /voguemind/checkpoints /voguemind/models
```

### Vérification

```bash
docker exec voguemind-namenode hdfs dfs -ls /voguemind
```

Résultat obtenu :

```text
Found 4 items
/voguemind/checkpoints
/voguemind/models
/voguemind/processed
/voguemind/raw
```

## 6. Tests de fonctionnement

Plusieurs tests ont été réalisés pour valider le bon fonctionnement de l'infrastructure.

### 6.1. Test d'écriture et de lecture

Un fichier `test.txt` a été créé, transféré dans HDFS, puis relu.

Commande de lecture :

```bash
docker exec voguemind-namenode hdfs dfs -cat /voguemind/raw/test.txt
```

Résultat obtenu :

```text
VogueMind HDFS test
```

Ce test confirme que le système permet d'écrire et de récupérer les données.

### 6.2. Test de réplication

```bash
docker exec voguemind-namenode hdfs fsck /voguemind/raw/test.txt -files -blocks -locations
```

Résultats obtenus :

```text
replication=2
Live_repl=2
Status: HEALTHY
Number of data-nodes: 2
Under-replicated blocks: 0
Missing blocks: 0
Corrupt blocks: 0
```

Le fichier est correctement répliqué sur les deux DataNodes, sans bloc manquant ou corrompu.

### 6.3. Test de persistance

Un test a été réalisé pour vérifier la conservation des données après suppression et recréation des conteneurs.

```bash
docker compose -f infra/docker-compose.hdfs.yml down
docker compose -f infra/docker-compose.hdfs.yml up -d
```

Après la reconnexion des DataNodes, le fichier de test est resté accessible.

Ce résultat confirme le fonctionnement des volumes Docker persistants.

## 7. Intégration avec Kafka et Spark

L'infrastructure HDFS constitue la couche de stockage du pipeline Big Data VogueMind.

- **Apache Kafka :** ingestion des données.
- **Apache Spark :** traitement et analyse des données.
- **Apache Hadoop HDFS :** stockage distribué des données brutes et traitées.

Adresse HDFS interne :

```text
hdfs://namenode:9000
```

L'intégration complète avec Kafka et Spark reste à réaliser et à valider collectivement.

## 8. Arrêt du cluster

Pour arrêter l'infrastructure sans supprimer les volumes :

```bash
docker compose -f infra/docker-compose.hdfs.yml down
```

**Attention :** ne pas utiliser `down -v`, car cette commande supprimerait également les volumes Docker du projet.

## 9. Captures d'écran

### 9.1. Déploiement du cluster Hadoop

Les trois conteneurs Docker ont été démarrés avec succès et présentent l'état `healthy`.

\<img width="1316" height="576" alt="Déploiement du cluster Hadoop" src="[https://github.com/user-attachments/assets/b816b35f-7939-4614-b737-29c4f9b5cff3](https://github.com/user-attachments/assets/b816b35f-7939-4614-b737-29c4f9b5cff3)" />

### 9.2. Stockage HDFS et état du NameNode

L'interface Web Hadoop confirme que deux nœuds de stockage sont en service.

\<img width="1572" height="502" alt="Interface NameNode HDFS" src="[https://github.com/user-attachments/assets/6efe2349-9fde-4071-ad61-30a7a1eeb4ce](https://github.com/user-attachments/assets/6efe2349-9fde-4071-ad61-30a7a1eeb4ce)" />

### 9.3. Organisation des répertoires HDFS

Les quatre répertoires `raw`, `processed`, `checkpoints` et `models` ont été créés.

\<img width="852" height="212" alt="Répertoires HDFS" src="[https://github.com/user-attachments/assets/5afe36a5-4374-4673-bb01-74fbdaf9ef18](https://github.com/user-attachments/assets/5afe36a5-4374-4673-bb01-74fbdaf9ef18)" />

### 9.4. Vérification de la réplication

Le test `hdfs fsck` confirme deux répliques effectives et un état global `HEALTHY`.

\<img width="1287" height="533" alt="Réplication HDFS" src="[https://github.com/user-attachments/assets/45780cba-585b-49be-99bf-68ad6f596813](https://github.com/user-attachments/assets/45780cba-585b-49be-99bf-68ad6f596813)" />

### 9.5. Importation et lecture du dataset H&M — Semaine 2

Un échantillon de 1 000 articles a été extrait du fichier `articles.csv` du dataset H&M, puis transféré vers Hadoop HDFS.

**Caractéristiques du fichier :**

- Nom : `articles_sample.csv`
- Nombre d'articles : 1 000
- Taille : 355 997 octets
- Chemin HDFS : `/voguemind/raw/articles_sample.csv`

**Commandes utilisées :**

```bash
docker cp ./data/articles_sample.csv voguemind-namenode:/tmp/articles_sample.csv
docker exec voguemind-namenode hdfs dfs -put -f /tmp/articles_sample.csv /voguemind/raw/
docker exec voguemind-namenode hdfs dfs -cat /voguemind/raw/articles_sample.csv
```

**Résultat :** le contenu CSV est correctement accessible depuis HDFS, avec les informations des articles H&M (identifiants, noms, types de produits, couleurs et descriptions).

**Capture : lecture des données H&M dans HDFS**

\<!-- Insérer ici la première capture image(20261108-011105).png depuis GitHub -->

### 9.6. Vérification de l'intégrité du dataset H&M — Semaine 2

Après l'importation, une vérification a été effectuée à l'aide de la commande :

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

Le fichier est correctement stocké dans HDFS et le contrôle d'intégrité ne signale aucun problème. La configuration HDFS prévoit un facteur de réplication de 2.



---

**Projet :** VogueMind
**Composant :** Infrastructure Hadoop HDFS
**Statut :** Cluster déployé et validé localement — échantillon H&M importé et vérifié — intégration Kafka/Spark à venir.
