with transactions as (

    select * 
    from {{ ref('fct_transactions') }}

),

comptes as (

    select *
    from {{ ref('dim_comptes') }}

),

solde_mensuel as (

    select
        compte_id,
        mois_transaction,

        sum(
            case
                when type_operation = 'credit'
                then montant
                else 0
            end
        ) as total_credits,

        sum(
            case
                when type_operation = 'debit'
                then montant
                else 0
            end
        ) as total_debits,

        sum(montant_signe) as solde_net_mois

    from transactions

    group by
        compte_id,
        mois_transaction

)

select
    s.compte_id,
    s.mois_transaction,

    s.total_credits,
    s.total_debits,
    s.solde_net_mois,

    c.solde_initial,

    c.solde_initial
    + sum(s.solde_net_mois) over (
        partition by s.compte_id
        order by s.mois_transaction
        rows between unbounded preceding and current row
    ) as solde_cumule

from solde_mensuel s

left join comptes c
    on s.compte_id = c.compte_id