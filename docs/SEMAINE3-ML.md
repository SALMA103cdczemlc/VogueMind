# Semaine 3 — Préparation, entraînement, évaluation et sauvegarde du pipeline ML

Réalisée par : Nada Rian (Spark et traitements)

Jalon de la fiche : **modèle évalué, versionné et prêt pour l'inférence**.

## 1. Cas métier et tâche ML

**Question :** ce client va-t-il racheter chez H&M dans les 7 prochains jours ?

**Tâche :** classification binaire (une seule tâche ML, comme demandé par la fiche).

**Pourquoi le ML ?** Les indicateurs de la semaine 2 décrivent le passé. Avec 1,3 million de clients, aucune règle écrite à la main ne peut anticiper le comportement de chacun. Le modèle apprend, à partir de l'historique stocké dans HDFS, les comportements qui précèdent un réachat. Il est ensuite utilisé en temps réel (semaine 4) : dès qu'un client achète, on estime s'il reviendra.

**Usage :** les clients à forte probabilité sont des cibles pour des recommandations personnalisées. Ceux à faible probabilité peuvent recevoir une relance (offre, nouveauté) pour éviter de les perdre.

## 2. Données documentées

| Élément | Description |
|---|---|
| Provenance | Dataset *H&M Personalized Fashion Recommendations* publié par H&M sur Kaggle (compétition 2022). Usage académique, non commercial, dans le cadre des règles de la compétition. |
| Fichiers | `transactions_train.csv` (≈ 31,8 M achats, 2018-09-20 → 2020-09-22), `articles.csv` (≈ 105 000 articles), `customers.csv` (≈ 1,37 M clients) |
| Sens d'un événement | Un achat d'un article par un client un jour donné (une ligne = un article acheté) |
| Identifiant | `transaction_id` attribué par le producer (numéro de ligne du fichier) ; `customer_id`, `article_id` pour les entités |
| Horodatage | `t_dat` : date d'achat (au jour près). `ingestion_timestamp` : heure d'envoi dans Kafka |
| Prix | `price` normalisé par Kaggle (entre 0 et 1), donc pas de montant en euros |
| Volume utilisé | Fenêtre de 12 semaines d'historique (paramétrable), ≈ 300 000 transactions par semaine |
| Cible | `label = 1` si le client fait au moins un achat dans `[cutoff ; cutoff + 7 jours[` |

**Règle anti-fuite :** à une date de coupure `cutoff`, les variables n'utilisent que les achats **strictement antérieurs** à `cutoff`, c'est-à-dire l'information réellement disponible au moment de la prédiction.

## 3. Variables explicatives (`batch/features.py`)

Calculées sur les 28 jours précédant la coupure, pour chaque client actif sur cette période :

| Variable | Sens |
|---|---|
| `recency_days` | Jours depuis le dernier achat |
| `tenure_days` | Jours depuis le premier achat de la fenêtre |
| `n_tx_7d`, `n_tx_28d` | Nombre d'articles achetés sur 7 et 28 jours |
| `n_days_28d` | Nombre de jours d'achat distincts |
| `spend_7d`, `spend_28d`, `avg_price` | Dépenses et prix moyen (prix normalisés) |
| `online_share` | Part des achats en ligne (canal 2) |
| `n_articles_28d`, `n_groups_28d` | Diversité des articles et des groupes de produits |
| `age`, `club_member_status`, `fashion_news_frequency` | Profil client (`customers.csv`), « inconnu » si absent |

Le **même module** sert à l'entraînement et à l'inférence temps réel : les variables sont calculées exactement de la même façon des deux côtés.

## 4. Découpage temporel

Les coupures sont hebdomadaires dans l'historique (ex. 12 semaines → 8 coupures) :

| Jeu | Coupures | Rôle |
|---|---|---|
| Entraînement | toutes sauf les 2 dernières | apprentissage |
| Validation | avant-dernière | comparaison des 3 techniques et réglage |
| Test | dernière | évaluation finale, jamais vue avant |

Le découpage n'est **jamais aléatoire** : on prédit toujours le futur à partir du passé. Toutes les transformations (encodage des catégories, normalisation) sont dans le `Pipeline` Spark ML : elles sont donc **apprises sur l'entraînement uniquement**. Le déséquilibre des classes est compensé par une pondération calculée sur l'entraînement.

## 5. Référence, trois techniques et réglage (`batch/train_model.py`)

- **Référence simple :** « le client a acheté dans les 7 derniers jours ⇒ il rachète ». Un modèle n'a d'intérêt que s'il fait mieux.
- **Trois techniques Spark ML :** régression logistique, forêt aléatoire (Random Forest), gradient boosting (GBT).
- **Réglage :** la technique avec le meilleur F1 en validation est réglée sur une grille de paramètres :

| Technique | Paramètres testés |
|---|---|
| Régression logistique | `regParam` ∈ {0 ; 0,01 ; 0,1}, `elasticNetParam` ∈ {0 ; 0,5} |
| Forêt aléatoire | `numTrees` ∈ {40 ; 80}, `maxDepth` ∈ {6 ; 10}, `minInstancesPerNode` ∈ {1 ; 20} |
| Gradient boosting | `maxIter` ∈ {30 ; 60}, `maxDepth` ∈ {3 ; 5}, `stepSize` ∈ {0,05 ; 0,1} |

- **Final :** les meilleurs paramètres sont réentraînés sur entraînement + validation, puis évalués une seule fois sur le test.

**Métriques :** matrice de confusion, précision, rappel, F1 (classe « réachat ») et AUC.

**Lecture des erreurs :**
- **Faux positif :** on prédit un réachat qui n'a pas lieu. Le client ne reçoit pas de relance alors qu'il en aurait eu besoin : risque de le perdre.
- **Faux négatif :** on prédit l'absence de réachat alors que le client revient. On lui envoie une relance inutile : coût marketing, mais faible impact.

Le F1 équilibre ces deux erreurs. On peut ensuite déplacer le seuil de décision (0,5 par défaut) selon la priorité métier.

## 6. Sauvegarde et versionnage

| Élément | Emplacement |
|---|---|
| Pipeline complet (préparation + modèle) | HDFS `/voguemind/models/repurchase_v1/` |
| Rapport JSON dans HDFS | `/voguemind/models/repurchase_v1_metrics/` |
| Rapport lisible | `results/ml_metrics_v1.md` et `results/ml_metrics_v1.json` (dépôt GitHub) |

Une nouvelle version s'obtient avec `VERSION=v2 ./scripts/run_week3.sh`. Le job temps réel choisit sa version avec `--version`.

## 7. Lancement

```bash
# data/ : transactions_train.csv, articles.csv, customers.csv
./scripts/run_week3.sh
```

Le script enchaîne : historique → référentiels → indicateurs → entraînement → export vers PostgreSQL.

Paramètres : `START=2018-09-20 WEEKS=12 PCT=5 VERSION=v1 ./scripts/run_week3.sh`. `PCT` est le pourcentage de clients utilisés pour l'entraînement : 5 % tient sur deux workers de 1 Go.

## 8. Résultats et interprétation

Les résultats de l'exécution sur le cluster sont dans [`results/ml_metrics_v1.md`](../results/ml_metrics_v1.md) : tableau de comparaison, grille de réglage, matrice de confusion du test.

Points à commenter dans le rapport :
1. le gain du modèle réglé par rapport à la référence (F1, précision) ;
2. l'écart entre validation et test, qui indique la stabilité dans le temps ;
3. les variables les plus utiles : la récence et la fréquence dominent généralement ce type de problème.

## 9. Limites

- Les prix sont normalisés et la date d'achat est au jour près, ce qui empêche les variables à l'heure près.
- La fenêtre est de 12 semaines, avec un échantillon de clients pour tenir sur le cluster local : les performances peuvent évoluer sur l'historique complet de 2 ans (saisonnalité).
- Les clients sans achat sur les 28 derniers jours ne sont pas scorés (pas de variables) : il faudrait une stratégie dédiée, par exemple un score de popularité.
- Le modèle doit être réentraîné régulièrement : la mode change vite (dérive des données).
