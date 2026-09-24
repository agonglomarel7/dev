SELECT
    ID AS categorie_id,
    NOM AS nom_categorie,
    "TYPE" AS type_categorie,
    GROUPE AS groupe,
    _LOADED_AT AS loaded_at

FROM {{ source('fintrack_raw', 'RAW_CATEGORIES') }}