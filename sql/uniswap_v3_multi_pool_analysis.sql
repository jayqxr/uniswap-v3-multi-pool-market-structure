WITH token_of_interest AS (
    SELECT from_hex('dd3B11eF34cd511a2DA159034a05fcb94D806686') AS toi
),

pool_created AS (
    SELECT 
        evt_block_time, 
        contract_address AS pool_contract, 
        token0, 
        token1,
        fee AS swap_fee,
        tickSpacing,
        pool AS pool_address
    FROM uniswap_v3_multichain.factory_evt_poolcreated
    WHERE chain = 'ethereum'
),

target_pool AS (
    SELECT
        p.*,
        t.toi
    FROM pool_created p
    JOIN token_of_interest t
      ON (p.token0 = t.toi OR p.token1 = t.toi)
),

swap_event AS (
    SELECT 
        evt_block_time,
        contract_address AS pool_contract, 
        amount0, 
        amount1, 
        sqrtPriceX96,
        tick AS price_tick
    FROM uniswap_v3_multichain.pair_evt_swap
    WHERE chain = 'ethereum'
      AND contract_address IN (SELECT pool_address FROM target_pool)
      AND evt_block_time > CURRENT_DATE - INTERVAL '365' DAY
),

token_metadata AS (
    SELECT contract_address, decimals
    FROM tokens.erc20
),

swap_with_pool AS (
    SELECT
        s.*, 
        p.pool_address,
        p.token0, 
        p.token1, 
        p.swap_fee, 
        p.tickSpacing,
        p.toi,
        COALESCE(t0.decimals, 18) AS decimals_token0,
        COALESCE(t1.decimals, 18) AS decimals_token1,
        s.amount0 / POWER(10, COALESCE(t0.decimals,18)) AS amount0_norm,
        s.amount1 / POWER(10, COALESCE(t1.decimals,18)) AS amount1_norm,
       
        POWER(CAST(s.sqrtPriceX96 AS DOUBLE) / POWER(2,96), 2)
            * POWER(10, COALESCE(t0.decimals,18) - COALESCE(t1.decimals,18))
            AS price_token1_per_token0

    FROM swap_event s
    JOIN target_pool p
        ON s.pool_contract = p.pool_address
    LEFT JOIN token_metadata t0 ON p.token0 = t0.contract_address
    LEFT JOIN token_metadata t1 ON p.token1 = t1.contract_address
),

pool_month_agg AS (
    SELECT
        DATE_TRUNC('month', evt_block_time) AS months,
        pool_address,
        swap_fee,
        tickSpacing,
        COUNT(*) AS swap_count,
        SUM(amount0_norm) AS total_amount0,
        SUM(amount1_norm) AS total_amount1,
        SUM(
            CASE WHEN token1 = toi THEN amount1_norm / NULLIF(price_token1_per_token0,0)
                 ELSE 0
            END
        ) AS memecoin_flow,

        SUM(
            CASE WHEN token1 = toi THEN (1/NULLIF(price_token1_per_token0,0)) * amount1_norm
                 ELSE 0
            END
        ) /
        NULLIF(
            SUM(
                CASE WHEN token1 = toi THEN amount1_norm ELSE 0 END
            ), 0
        ) * 1e18 AS VWAP,

        MAX(price_token1_per_token0) AS highest_price_token1_per_token0,
        MAX(price_tick) AS highest_price_tick

    FROM swap_with_pool sw
    GROUP BY 1,2,3,4
),

global_month_agg AS (
    SELECT
        months,
        SUM(total_amount0) AS global_total_amount0,
        SUM(total_amount1) AS global_total_amount1,
        SUM(swap_count) AS global_swap_count,
        SUM(memecoin_flow) AS global_memecoin_flow,
        SUM(VWAP * total_amount1) / NULLIF(SUM(total_amount1),0) AS global_VWAP

    FROM pool_month_agg
    GROUP BY months
),

final_unified AS (
    SELECT
        months,
        'pool' AS level,
        pool_address,
        swap_fee,
        tickSpacing,
        swap_count,
        total_amount0,
        total_amount1,
        memecoin_flow,
        VWAP,
        highest_price_token1_per_token0,
        highest_price_tick,

        NULL AS global_total_amount0,
        NULL AS global_total_amount1,
        NULL AS global_swap_count,
        NULL AS global_memecoin_flow,
        NULL AS global_VWAP

    FROM pool_month_agg

    UNION ALL

    SELECT
        months,
        'global' AS level,
        NULL AS pool_address,
        NULL AS swap_fee,
        NULL AS tickSpacing,
        NULL AS swap_count,
        NULL AS total_amount0,
        NULL AS total_amount1,
        NULL AS memecoin_flow,
        NULL AS VWAP,
        NULL AS highest_price_token1_per_token0,
        NULL AS highest_price_tick,

        global_total_amount0,
        global_total_amount1,
        global_swap_count,
        global_memecoin_flow,
        global_VWAP

    FROM global_month_agg
)

SELECT *
FROM final_unified
ORDER BY months, level;
