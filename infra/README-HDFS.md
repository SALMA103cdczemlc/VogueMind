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


## 10. Conclusion

Cette partie du projet VogueMind m'a permis de mettre en place et de tester une infrastructure de stockage distribué avec Hadoop HDFS et Docker.

J'ai commencé par configurer le NameNode et les deux DataNodes, puis j'ai vérifié leur fonctionnement, la réplication des fichiers et la persistance des données.

Ensuite, nous avons importé un échantillon du dataset H&M dans HDFS et vérifié son intégrité.

Enfin, nous avons testé l'intégration avec Apache Spark en écrivant et en relisant 10 000 lignes au format Parquet.

Les tests ont été réalisés avec succès. L'infrastructure HDFS est maintenant fonctionnelle et peut être utilisée pour la suite du projet VogueMind.

La prochaine étape consiste à poursuivre l'intégration des différents composants du pipeline Big Data, notamment Kafka, Spark et HDFS.

---

**Projet :** VogueMind  
**Composant :** Infrastructure Hadoop HDFS  
**Réalisée par :** Salma Hilali  
**Statut :** Cluster HDFS opérationnel, dataset H&M importé et intégration Spark-HDFS validée.
