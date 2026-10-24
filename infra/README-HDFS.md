# Hadoop HDFS — VogueMind 

Réalisée par : Salma Hilali

Responsabilité : Infrastructure Hadoop HDFS — Docker

## 1. Présentation

Dans le cadre du projet **VogueMind**, cette partie consiste à mettre en place une infrastructure de stockage distribué avec **Apache Hadoop HDFS**, déployée à l'aide de Docker Compose.

L'objectif est de disposer d'un environnement de stockage fiable, répliqué et persistant, destiné à accueillir les données du pipeline Big Data intégrant Apache Kafka et Apache Spark.

## 2. Architecture HDFS

L'infrastructure repose sur un cluster Hadoop composé de trois conteneurs Docker :

| Composant | Conteneur | Rôle |
|---|---|---|
| NameNode | `voguemind-namenode` | Gestion des métadonnées et de l'organisation des fichiers |
| DataNode 1 | `voguemind-datanode1` | Stockage des blocs de données |
| DataNode 2 | `voguemind-datanode2` | Stockage des blocs et réplication |

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

| Répertoire | Utilisation prévue |
|---|---|
| `/voguemind/raw` | Stockage des données brutes |
| `/voguemind/processed` | Stockage des données transformées par Spark |
| `/voguemind/checkpoints` | Sauvegarde des checkpoints des traitements streaming |
| `/voguemind/models` | Stockage des modèles analytiques et de recommandation |

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

La commande suivante permet de vérifier l'intégrité du fichier et la localisation de ses blocs :

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

Arrêt et suppression des conteneurs :

```bash
docker compose -f infra/docker-compose.hdfs.yml down
```

Redémarrage :

```bash
docker compose -f infra/docker-compose.hdfs.yml up -d
```

Après la reconnexion des DataNodes, le fichier de test est resté accessible.

Ce résultat confirme le fonctionnement des volumes Docker persistants.

## 7. Intégration avec Kafka et Spark

L'infrastructure HDFS constitue la couche de stockage du pipeline Big Data VogueMind.

Elle est destinée à être utilisée avec les autres composants du projet :

- **Apache Kafka :** ingestion des données.
- **Apache Spark :** traitement et analyse des données.
- **Apache Hadoop HDFS :** stockage distribué des données brutes et traitées.

Pour les services connectés au réseau Docker `voguemind-net`, l'adresse du cluster est :

```text
hdfs://namenode:9000
```

L'intégration complète avec Kafka et Spark reste à réaliser et à valider collectivement.

## 8. Arrêt du cluster

Pour arrêter l'infrastructure sans supprimer les volumes :

```bash
docker compose -f infra/docker-compose.hdfs.yml down
```

**Attention :** ne pas utiliser `down -v`, car cette commande supprimerait également les volumes Docker du projet, avec un risque de perte des données HDFS.

## 9. Captures d'écran

Les captures suivantes présentent les principales étapes de déploiement et de validation de l'infrastructure.

### 9.1. Déploiement du cluster Hadoop

Les trois conteneurs Docker (NameNode et deux DataNodes) ont été démarrés avec succès et présentent l'état `healthy`.
<img width="1316" height="576" alt="image" src="https://github.com/user-attachments/assets/b816b35f-7939-4614-b737-29c4f9b5cff3" />


### 9.2. Stockage HDFS et état du NameNode

L'interface Web Hadoop confirme que le répertoire de stockage du NameNode est actif et que deux nœuds de stockage sont en service.

<img width="1572" height="502" alt="image" src="https://github.com/user-attachments/assets/6efe2349-9fde-4071-ad61-30a7a1eeb4ce" />


### 9.3. Organisation des répertoires HDFS

Les quatre répertoires `raw`, `processed`, `checkpoints` et `models` ont été créés pour structurer le stockage des données.

<img width="852" height="212" alt="image" src="https://github.com/user-attachments/assets/5afe36a5-4374-4673-bb01-74fbdaf9ef18" />


### 9.4. Vérification de la réplication

Le test `hdfs fsck` confirme la présence de deux répliques effectives (`Live_repl=2`) et un état global `HEALTHY`.

<img width="1287" height="533" alt="image" src="https://github.com/user-attachments/assets/45780cba-585b-49be-99bf-68ad6f596813" />


---

**Projet :** VogueMind  
**Composant :** Infrastructure Hadoop HDFS  
**Statut :** Cluster déployé et validé localement — intégration Kafka/Spark à venir.
