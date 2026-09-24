with budgets as (

    select
        compte_id,
        categorie_id,
        mois,
        montant_prevu

    from {{ ref('stg_budgets') }}

),

depenses_reelles as (

    select
        compte_id,
        categorie_id,
        mois_transaction as mois,

        sum(montant) as montant_reel

    from {{ ref('fct_transactions') }}

    where type_operation = 'debit'

    group by
        compte_id,
        categorie_id,
        mois_transaction

),

categories as (

    select *
    from {{ ref('dim_categories') }}

),

budget_vs_reel as (

    select

        coalesce(b.compte_id, d.compte_id) as compte_id,

        coalesce(
            b.categorie_id,
            d.categorie_id
        ) as categorie_id,

        coalesce(b.mois, d.mois) as mois,

        b.montant_prevu,

        d.montant_reel

    from budgets b

    full outer join depenses_reelles d
        on b.compte_id = d.compte_id
        and b.categorie_id = d.categorie_id
        and b.mois = d.mois

)

select
    bvr.compte_id,

    c.nom_categorie,

    bvr.categorie_id,

    bvr.mois,

    coalesce(
        bvr.montant_prevu,
        0
    ) as montant_prevu,

    coalesce(
        bvr.montant_reel,
        0
    ) as montant_reel,

    coalesce(
        bvr.montant_reel,
        0
    )
    -
    coalesce(
        bvr.montant_prevu,
        0
    ) as ecart,

    case
        when coalesce(bvr.montant_reel, 0)
             >
             coalesce(bvr.montant_prevu, 0)

        then true

        else false

    end as depassement

from budget_vs_reel bvr

left join categories c
    on bvr.categorie_id = c.categorie_id