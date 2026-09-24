SELECT
    categorie_id,
    nom_categorie,
    type_categorie,
    groupe

FROM {{ ref('stg_categories') }}