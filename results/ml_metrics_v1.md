# Résultats ML — modèle de réachat v1

Exécution : 2026-11-22T14:26:13

## Données

- Historique : 2018-09-20 → 2018-12-12
- Coupures hebdomadaires : 2018-10-18, 2018-10-25, 2018-11-01, 2018-11-08, 2018-11-15, 2018-11-22, 2018-11-29, 2018-12-06
- Entraînement : 76852 lignes (coupures 2018-10-18, 2018-10-25, 2018-11-01, 2018-11-08, 2018-11-15, 2018-11-22)
- Validation : 13201 lignes (coupure 2018-11-29)
- Test : 13048 lignes (coupure 2018-12-06)
- Taux de réachat (entraînement) : 14.8%
- Échantillon de clients : 5 %

## Comparaison des techniques (validation)

| Modèle | Précision | Rappel | F1 | AUC | VP | FP | FN | VN |
|---|---|---|---|---|---|---|---|---|
| Référence (achat < 7 j) | 0.146 | 0.539 | 0.231 | — | 896 | 5218 | 765 | 6322 |
| regression_logistique | 0.197 | 0.524 | 0.287 | 0.637 | 870 | 3537 | 791 | 8003 |
| foret_aleatoire | 0.193 | 0.553 | 0.286 | 0.640 | 918 | 3837 | 743 | 7703 |
| gradient_boosting | 0.196 | 0.525 | 0.285 | 0.644 | 872 | 3579 | 789 | 7961 |

Technique retenue pour le réglage : **regression_logistique**

## Réglage (validation)

| Paramètres | F1 | AUC |
|---|---|---|
| {"regParam": 0.0, "elasticNetParam": 0.0} | 0.286 | 0.636 |
| {"regParam": 0.0, "elasticNetParam": 0.5} | 0.286 | 0.636 |
| {"regParam": 0.01, "elasticNetParam": 0.0} | 0.287 | 0.637 |
| {"regParam": 0.01, "elasticNetParam": 0.5} | 0.292 | 0.634 |
| {"regParam": 0.1, "elasticNetParam": 0.0} | 0.283 | 0.638 |
| {"regParam": 0.1, "elasticNetParam": 0.5} | 0.290 | 0.620 |

Meilleurs paramètres : `{"regParam": 0.01, "elasticNetParam": 0.5}`

## Évaluation finale (test, jamais vu)

| Modèle | Précision | Rappel | F1 | AUC | VP | FP | FN | VN |
|---|---|---|---|---|---|---|---|---|
| Référence (achat < 7 j) | 0.188 | 0.374 | 0.250 | — | 643 | 2787 | 1075 | 8543 |
| regression_logistique réglé | 0.207 | 0.458 | 0.285 | 0.632 | 787 | 3019 | 931 | 8311 |

Modèle sauvegardé : `hdfs://namenode:9000/voguemind/models/repurchase_v1`
