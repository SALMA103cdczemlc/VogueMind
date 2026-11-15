-- VogueMind — base de restitution (PostgreSQL)
-- Exécuté automatiquement au premier démarrage du conteneur postgres.

-- Prédictions temps réel écrites par streaming/predict_stream.py
-- Clé primaire sur transaction_id : un rejeu (reprise après panne) ne crée pas de doublon.
CREATE TABLE IF NOT EXISTS predictions (
    transaction_id    TEXT PRIMARY KEY,
    customer_id       TEXT        NOT NULL,
    article_id        TEXT        NOT NULL,
    t_dat             DATE        NOT NULL,
    price             DOUBLE PRECISION,
    sales_channel_id  INTEGER,
    proba_rachat      DOUBLE PRECISION NOT NULL,   -- probabilité de réachat sous 7 jours
    prediction        INTEGER     NOT NULL,        -- 1 = réachat prédit
    model_version     TEXT        NOT NULL,
    ingestion_ts      TIMESTAMPTZ,                 -- envoi par le producer
    kafka_ts          TIMESTAMPTZ,                 -- arrivée dans Kafka
    scored_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id          BIGINT
);
CREATE INDEX IF NOT EXISTS idx_predictions_scored_at ON predictions (scored_at);
CREATE INDEX IF NOT EXISTS idx_predictions_customer ON predictions (customer_id);

-- Les tables d'indicateurs (ind_*) sont créées par batch/export_to_postgres.py
