
$siteUid = plallegromarketplace
$imageheaders = image, image2, image3, image4, image5, image6, image7, image8, image9, image10, image11, image12, image13, image14, image15, image16
$headers = sku, localization, name, $imageheaders, description, parent_sku, subcategory, category, supercategory, product_type, color, size, product_url, shipping_weight, ean, brand, marketplaceId, currency, msrp, cost, fulfillmentPolicyId, aspect_names, aspect_values, flag, shipToQuantity, parent_sku_name, tv_type
$query = SELECT * FROM ( {{ SELECT {tvp.code} AS sku, 'pl_PL' AS localization, CASE WHEN {tvp.name[pl_PL] : o} IS NULL THEN {tvp.code} ELSE {tvp.name[pl_PL] : o} END AS name, '' AS image, '' AS image2, '' AS image3, '' AS image4, '' AS image5, '' AS image6, '' AS image7, '' AS image8, '' AS image9, '' AS image10, '' AS image11, '' AS image12, '' AS image13, '' AS image14, '' AS image15, '' AS image16, CASE WHEN {tvp.description[pl_PL] : o} IS NULL THEN {base.description[pl_PL] : o} ELSE {tvp.description[pl_PL] : o} END AS description, {base.code} AS parent_sku, {catg.code} AS subcategory, {catg2.code} AS category, {catg3.code} AS supercategory, {ptype.code} AS product_type, {swc.colorName} AS color, {tvp.size[pl_PL] : o} AS size, {tvp.contentUrl[pl_PL] : o} AS product_url, {tvp.skuWeight} AS shipping_weight, {tvp.ean} AS ean, 'SAMSUNG' AS brand, 'PL_Allegro_Marketplace' AS marketplaceId, ( {{ SELECT TOP 1 {cr.isocode} FROM {PriceRow AS pr JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype} JOIN Currency AS cr ON {pr.currency} = {cr.pk}} WHERE {pr.product} = {tvp.pk} AND {pt.code} = 'RRP' AND {pr.minqtd} > 0 AND ( ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} IS NULL AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} IS NULL ) OR ( {pr.startTime} IS NULL AND {pr.endTime} IS NULL ) ) GROUP BY {pr.product}, {cr.isocode} }} ) AS currency, ( {{ SELECT MIN ({pr.price}) FROM {PriceRow AS pr JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype}} WHERE {pr.product} = {tvp.pk} AND {pt.code} = 'RRP' AND {pr.minqtd} > 0 AND ( ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} IS NULL AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} IS NULL ) OR ( {pr.startTime} IS NULL AND {pr.endTime} IS NULL ) ) GROUP BY {pr.product}}} ) AS msrp, ( CASE WHEN EXISTS ( {{ SELECT MIN ({pr.price}) FROM {PriceRow AS pr JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype} JOIN TokoMultiStore AS tms ON {tms.multistorepricegroup} = {pr.ug}} where {pr.product} = {tvp.pk} AND {tms.name} = 'plallegromarketplace' AND {pt.code} = 'SPECIAL' AND {pr.minqtd} > 0 AND ( ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} IS NULL AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} IS NULL ) OR ( {pr.startTime} IS NULL AND {pr.endTime} IS NULL ) ) GROUP BY {pr.product}}} ) THEN ( {{ SELECT MIN ({pr.price}) FROM {PriceRow AS pr JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype} JOIN TokoMultiStore AS tms ON {tms.multistorepricegroup} = {pr.ug}} WHERE {pr.product} = {tvp.pk} AND {tms.name} = 'plallegromarketplace' AND {pt.code} = 'SPECIAL' AND {pr.minqtd} > 0 AND ( ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} IS NULL AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} IS NULL ) OR ( {pr.startTime} IS NULL AND {pr.endTime} IS NULL ) ) GROUP BY {pr.product}}} ) ELSE ( {{ SELECT MIN ({pr.price}) FROM {PriceRow AS pr JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype}} WHERE {pr.product} = {tvp.pk} AND {pt.code} = 'RRP' AND {pr.minqtd} > 0 AND ( ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} IS NULL AND {pr.endTime} >= CURRENT_TIMESTAMP ) OR ( {pr.startTime} <= CURRENT_TIMESTAMP AND {pr.endTime} IS NULL ) OR ( {pr.startTime} IS NULL AND {pr.endTime} IS NULL ) ) GROUP BY {pr.product}}} ) END ) AS cost, ( CASE WHEN EXISTS ( {{ SELECT {zdm.code} FROM { BaseSite2DeliveryModeRel AS rel JOIN CMSSite AS s ON {rel.source} = {s.pk} JOIN ZoneDeliveryMode AS zdm ON {rel.target} = {zdm.pk} } WHERE {s.uid} = 'plallegromarketplace' }} ) THEN ( {{ SELECT TOP 1 {zdm.code} FROM {ProductDeliveryModeRelation AS pdmr JOIN ZoneDeliveryMode AS zdm ON {pdmr.target} = {zdm.pk} LEFT JOIN BaseSite2DeliveryModeRel AS siteRel ON {zdm.pk} = {siteRel.target} LEFT JOIN CMSSite AS site ON {siteRel.source} = {site.pk}} WHERE {pdmr.source} = {tvp.pk} AND {site.uid} = 'plallegromarketplace' }} ) ELSE ( {{ SELECT TOP 1 {zdm.code} FROM {ProductDeliveryModeRelation AS pdmr JOIN ZoneDeliveryMode AS zdm ON {pdmr.target} = {zdm.pk} LEFT JOIN BaseStore2DeliveryModeRel AS storeRel ON {zdm.pk} = {storeRel.target} LEFT JOIN BaseStore AS store ON {storeRel.source} = {store.pk}} WHERE {pdmr.source} = {tvp.pk} AND {store.uid} = 'pl' }} ) END ) AS fulfillmentPolicyId, CONCAT( 'Brand|Model|Color|Size|', ( {{ SELECT STRING_AGG( CONVERT( NVARCHAR(max), CONCAT( {cc.name[pl_PL] : o}, '.', {ca.name[pl_PL] : o} ) ), '|' ) FROM { ProductFeature AS pf JOIN ClassAttributeAssignment AS caa ON {caa.pk} = {pf.classificationAttributeAssignment} JOIN ClassificationClass AS cc ON {cc.pk} = {caa.classificationClass} JOIN ClassificationAttribute AS ca ON {ca.pk} = {caa.classificationAttribute} JOIN ClassificationAttributeVisibilityEnum AS cave ON {cave.pk} = {caa.visibility} } WHERE {pf.product} = {tvp.pk} AND {cave.code} = 'visible' }} ) ) AS aspect_names, CONCAT( 'SAMSUNG|', {tvp.code}, '|', CASE WHEN {swc.colorName} IS NULL THEN ' ' ELSE {swc.colorName} END, '|', CASE WHEN {tvp.size[pl_PL] : o} IS NULL THEN ' ' ELSE {tvp.size[pl_PL] : o} END, '|', ( {{ SELECT STRING_AGG( CONVERT( NVARCHAR(max), {pf.stringValue} ), '|' ) FROM { ProductFeature AS pf JOIN ClassAttributeAssignment AS caa ON {caa.pk} = {pf.classificationAttributeAssignment} JOIN ClassificationClass AS cc ON {cc.pk} = {caa.classificationClass} JOIN ClassificationAttribute AS ca ON {ca.pk} = {caa.classificationAttribute} JOIN ClassificationAttributeVisibilityEnum AS cave ON {cave.pk} = {caa.visibility} } WHERE {pf.product} = {tvp.pk} AND {cave.code} = 'visible' }} ) ) AS aspect_values, ( CASE WHEN {tvp.pk} NOT IN ( {{SELECT {mserel.target} FROM {TokoMultiStore2ExclusionProductRelation AS mserel JOIN TokoMultiStore AS tms ON {mserel.source} = {tms.pk}} WHERE {tms.name} = 'plallegromarketplace' }} ) AND {base.pk} NOT IN ( {{SELECT {mserel.target} FROM {TokoMultiStore2ExclusionProductRelation AS mserel JOIN TokoMultiStore AS tms ON {mserel.source} = {tms.pk}} WHERE {tms.name} = 'plallegromarketplace' }} ) AND {catRel.source} NOT IN ( {{SELECT {mserel.target} FROM {TokoMultiStore2ExclusionCategoryRelation AS mserel JOIN TokoMultiStore AS tms ON {mserel.source} = {tms.pk}} WHERE {tms.name} = 'plallegromarketplace' }} ) THEN 'Include' ELSE 'Exclude' END ) AS flag, ( {{ select ( CASE WHEN {sl.productcode} IN ( {{SELECT {slt.productcode} FROM {StockLevel AS slt JOIN InStockStatus AS isst ON {slt.instockstatus} = {isst.pk} and {isst.codeLowerCase} = 'forceinstock' } where {slt.productcode} = {tvp.code} and {slt.warehouse} in ( {{ select {pk} from {Warehouse} where ( EXISTS ( {{ select {rel.target} from { BaseSite2WarehouseRel as rel join CMSSite as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'plallegromarketplace' }} ) and {pk} in ( {{ select {rel.target} from { BaseSite2WarehouseRel as rel join CMSSite as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'plallegromarketplace' }} ) ) or ( NOT EXISTS ( {{ select {rel.target} from { BaseSite2WarehouseRel as rel join CMSSite as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'plallegromarketplace' }} ) and {pk} in ( {{ select {rel.target} from { BaseStore2WarehouseRel as rel join BaseStore as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'pl' }} ) ) or ( EXISTS ( {{ select {virtualWarehouse} from {CMSSite} where {uid} = 'plallegromarketplace' and {virtualWarehouse} is not null}} ) and {pk} in ( {{ select {virtualWarehouse} from {CMSSite} where {uid} = 'plallegromarketplace' and {virtualWarehouse} is not null}} ) ) or ( NOT EXISTS ( {{ select {virtualWarehouse} from {CMSSite} where {uid} = 'plallegromarketplace' and {virtualWarehouse} is not null}} ) and {pk} in ( {{ select {virtualWarehouse} from {BaseStore} where {uid} = 'pl' and {virtualWarehouse} is not null}} ) ) }} ) }} ) OR SUM( COALESCE({sl.available}, 0) ) - SUM( COALESCE({sl.reserved}, 0) ) - SUM( COALESCE({sl.safetyStock}, 0) ) < 0 THEN 0 ELSE sum( COALESCE({sl.available}, 0) ) - sum( COALESCE({sl.reserved}, 0) ) - sum( COALESCE({sl.safetyStock}, 0) ) END ) from {StockLevel as sl left join InStockStatus as iss on {sl.instockstatus} = {iss.pk} and {iss.codeLowerCase} <> 'forceoutofstock' } where {sl.productcode} = {tvp.code} and {sl.warehouse} in ( {{ select {pk} from {Warehouse} where ( EXISTS ( {{ select {rel.target} from { BaseSite2WarehouseRel as rel join CMSSite as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'plallegromarketplace' }} ) and {pk} in ( {{ select {rel.target} from { BaseSite2WarehouseRel as rel join CMSSite as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'plallegromarketplace' }} ) ) or ( NOT EXISTS ( {{ select {rel.target} from { BaseSite2WarehouseRel as rel join CMSSite as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'plallegromarketplace' }} ) and {pk} in ( {{ select {rel.target} from { BaseStore2WarehouseRel as rel join BaseStore as bs on {rel.source} = {bs.pk} } where {bs.uid} = 'pl' }} ) ) or ( EXISTS ( {{ select {virtualWarehouse} from {CMSSite} where {uid} = 'plallegromarketplace' and {virtualWarehouse} is not null}} ) and {pk} in ( {{ select {virtualWarehouse} from {CMSSite} where {uid} = 'plallegromarketplace' and {virtualWarehouse} is not null}} ) ) or ( NOT EXISTS ( {{ select {virtualWarehouse} from {CMSSite} where {uid} = 'plallegromarketplace' and {virtualWarehouse} is not null}} ) and {pk} in ( {{ select {virtualWarehouse} from {BaseStore} where {uid} = 'pl' and {virtualWarehouse} is not null}} ) ) }} ) group by {sl.productcode}}} ) AS shipToQuantity, CASE WHEN {base.name[pl_PL] : o} IS NULL THEN {base.code} ELSE {base.name[pl_PL] : o} END AS parent_sku_name, CASE WHEN {tvp.code} IN ('QE32LS03BBUXXH', 'QE65LS01BAUXXH', 'QE65LS01BBUXXH', 'QE75LS03BAUXXH', 'QE85LS03BAUXXH', 'QE43QN91BATXXH', 'QE50QN91BATXXH', 'QE55QN700BTXXH', 'QE55QN85BATXXH', 'QE55QN91BATXXH', 'QE65QN700BTXXH', 'QE65QN800BTXXH', 'QE65QN85BATXXH', 'QE65QN900BTXXH', 'QE65QN91BATXXH', 'QE75QN700BTXXH', 'QE75QN800BTXXH', 'QE75QN85BATXXH', 'QE75QN900BTXXH', 'QE75QN91BATXXH', 'QE85QN800BTXXH', 'QE85QN85BATXXH', 'QE85QN900BTXXH', 'QE85QN90BATXXH', 'QE43Q67BAUXXH', 'QE50Q67BAUXXH', 'QE55Q67BAUXXH', 'QE55Q77BATXXH', 'QE55Q80BATXXH', 'QE65Q67BAUXXH', 'QE65Q77BATXXH', 'QE65Q80BATXXH', 'QE75Q67BAUXXH', 'QE75Q77BATXXH', 'QE75Q80BATXXH', 'QE85Q60BAUXXH', 'QE85Q70BATXXH', 'QE85Q80BATXXH', 'QE55S95BATXXH', 'QE65S95BATXXH', 'QE55QN95BATXXH', 'QE65QN95BATXXH', 'QE75QN95BATXXH', 'QE85QN95BATXXH', 'QE43LS05BAUXXH', 'QE43LS05TCUXXH', 'QE43LS01BAUXXH', 'QE43LS01BBUXXH', 'QE43LS01TAUXXH', 'QE43LS01TBUXXH', 'QE43Q67AAUXXH', 'QE50LS01BAUXXH', 'QE50LS01BBUXXH', 'QE50LS01TAUXXH', 'QE50LS01TBUXXH', 'QE55LS01BAUXXH', 'QE55LS01BBUXXH', 'QE55LS01TBUXXH', 'QE50Q67AAUXXH', 'QE55Q67AAUXXH', 'QE55LS03AAUXXH', 'QE55LS03BAUXXH', 'QE55Q77AATXXH', 'QE55Q80AATXXH', 'QE55QN85AATXXH', 'QE55QN95AATXXH', 'QE65Q67AAUXXH', 'QE65LS03AAUXXH', 'QE65LS03BAUXXH', 'QE65QN800ATXXH', 'QE65Q80AATXXH', 'QE65Q77AATXXH', 'QE65QN85AATXXH', 'QE75Q67AAUXXH', 'QE75LS03AAUXXH', 'QE75Q77AATXXH', 'QE75QN800ATXXH', 'QE75QN85AATXXH', 'QE85Q80AATXXH', 'QE75QN91AATXXH', 'QE43LS03AAUXXH', 'QE43LS03BAUXXH', 'QE50LS03AAUXXH', 'QE50LS03BAUXXH', 'QE50QN91AATXXH', 'QE55LST7TCUXXH', 'QE55QN91AATXXH', 'QE65QN900ATXXH', 'QE65QN95AATXXH', 'QE75Q80AATXXH', 'QE75QN900ATXXH', 'QE85LS03AAUXXH', 'QE65LST7TCUXXH', 'QE75LST7TCUXXH', 'QE55LS01TAUXXH', 'QE50Q80AATXXH', 'QE85QN85AATXXH', 'QE65QN91AATXXH', 'QE75QN95AATXXH', 'QE85QN95AATXXH', 'QE43LS05TAUXXH', 'QE85QN800ATXXH', 'QE85QN900ATXXH', 'QE85Q60AAUXXH', 'QE85Q70AATXXH', 'QE85QN90AATXXH', 'QE32LS03TCUXXH') THEN 'Qled' WHEN {tvp.code} IN ('UE43AU8002KXXH', 'UE50AU8002KXXH', 'UE75AU8002KXXH', 'UE65AU8002KXXH', 'UE32T4002AKXXH', 'UE32T4302AKXXH', 'UE70AU8072UXXH', 'UE55AU8002KXXH', 'UE85AU8002KXXH', 'UE70AU8002KXXH', 'UE50TU7092UXXH', 'UE55TU7092UXXH', 'UE65TU7092UXXH', 'UE75TU7092UXXH', 'UE43AU7192UXXH', 'UE50AU7192UXXH', 'UE55AU7192UXXH', 'UE65AU7192UXXH', 'UE75AU7192UXXH', 'UE85AU7192UXXH', 'UE43BU8002KXXH', 'UE50BU8002KXXH', 'UE55BU8002KXXH', 'UE65BU8002KXXH', 'UE75BU8002KXXH', 'UE85BU8002KXXH', 'UE32T5302CKXXH') THEN 'LED' ELSE '' END AS tv_type, ROW_NUMBER () OVER ( PARTITION BY {tvp.code}, {tvp.baseProduct} ORDER BY {catg.code} DESC ) AS rowNum FROM {TokoVariantProduct AS tvp JOIN Product AS base ON {tvp.baseProduct} = {base.pk} JOIN CatalogVersion AS cv ON {tvp.catalogVersion} = {cv.pk} JOIN Catalog AS cat ON {cv.pk} = {cat.activeCatalogVersion} JOIN CategoryProductRelation AS catRel ON {catRel.target} = {base.pk} JOIN Category AS catg ON {catRel.source} = {catg.pk} JOIN CategoryType AS catype ON {catg.categoryType} = {catype.pk} JOIN ArticleApprovalStatus AS aas ON {tvp.approvalStatus} = {aas.pk} JOIN CategoryCategoryRelation AS ccr ON {ccr.target} = {catg.pk} JOIN Category AS catg2 ON {ccr.source} = {catg2.pk} JOIN CategoryType AS catype2 ON {catg2.categoryType} = {catype2.pk} JOIN ProductType AS ptype ON {ptype.pk} = {tvp.productType} LEFT JOIN SwatchColor AS swc ON {tvp.color} = {swc.pk} LEFT JOIN CategoryCategoryRelation AS ccr2 ON {ccr2.target} = {catg2.pk} LEFT JOIN Category AS catg3 ON {ccr2.source} = {catg3.pk}} WHERE {aas.code} = 'approved' AND {cat.id} = 'plallegromarketplaceCatalog' AND {cv.version} = 'Online' AND {catype.code} = 'NAV' AND {catype2.code} = 'NAV' AND {ptype.code} = 'NORMAL' }} ) AS temp WHERE rowNum = 1

    INSERT_UPDATE TokoSalesReportJob ; code[unique = true]                            ; headers  ; salesQuery
                                 ; toko-$siteUid-marketplace-productCatalogCSVJob ; $headers ; $query


SELECT
    *
FROM
    (
        {{
    SELECT
      {tvp.code} AS sku,
      'pl_PL' AS localization,
      CASE WHEN {tvp.name[pl_PL] : o} IS NULL THEN {tvp.code} ELSE {tvp.name[pl_PL] : o} END AS name,
      '' AS image,
      '' AS image2,
      '' AS image3,
      '' AS image4,
      '' AS image5,
      '' AS image6,
      '' AS image7,
      '' AS image8,
      '' AS image9,
      '' AS image10,
      '' AS image11,
      '' AS image12,
      '' AS image13,
      '' AS image14,
      '' AS image15,
      '' AS image16,
      CASE WHEN {tvp.description[pl_PL] : o} IS NULL THEN {base.description[pl_PL] : o} ELSE {tvp.description[pl_PL] : o} END AS description,
      {base.code} AS parent_sku,
      {catg.code} AS subcategory,
      {catg2.code} AS category,
      {catg3.code} AS supercategory,
      {ptype.code} AS product_type,
      {swc.colorName} AS color,
      {tvp.size[pl_PL] : o} AS size,
      {tvp.contentUrl[pl_PL] : o} AS product_url,
      {tvp.skuWeight} AS shipping_weight,
      {tvp.ean} AS ean,
      'SAMSUNG' AS brand,
      'PL_Allegro_Marketplace' AS marketplaceId,
      (
        {{
        SELECT
          TOP 1 {cr.isocode}
        FROM
          {PriceRow AS pr
          JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype}
          JOIN Currency AS cr ON {pr.currency} = {cr.pk}}
        WHERE
          {pr.product} = {tvp.pk}
          AND {pt.code} = 'RRP'
          AND {pr.minqtd} > 0
          AND (
            (
              {pr.startTime} <= CURRENT_TIMESTAMP
              AND {pr.endTime} >= CURRENT_TIMESTAMP
            )
            OR (
              {pr.startTime} IS NULL
              AND {pr.endTime} >= CURRENT_TIMESTAMP
            )
            OR (
              {pr.startTime} <= CURRENT_TIMESTAMP
              AND {pr.endTime} IS NULL
            )
            OR (
              {pr.startTime} IS NULL
              AND {pr.endTime} IS NULL
            )
          )
        GROUP BY
          {pr.product},
          {cr.isocode} }}
      ) AS currency,
      (
        {{
        SELECT
          MIN ({pr.price})
        FROM
          {PriceRow AS pr
          JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype}}
        WHERE
          {pr.product} = {tvp.pk}
          AND {pt.code} = 'RRP'
          AND {pr.minqtd} > 0
          AND (
            (
              {pr.startTime} <= CURRENT_TIMESTAMP
              AND {pr.endTime} >= CURRENT_TIMESTAMP
            )
            OR (
              {pr.startTime} IS NULL
              AND {pr.endTime} >= CURRENT_TIMESTAMP
            )
            OR (
              {pr.startTime} <= CURRENT_TIMESTAMP
              AND {pr.endTime} IS NULL
            )
            OR (
              {pr.startTime} IS NULL
              AND {pr.endTime} IS NULL
            )
          )
        GROUP BY
          {pr.product}}}
      ) AS msrp,
      (
        CASE WHEN EXISTS (
          {{
          SELECT
            MIN ({pr.price})
          FROM
            {PriceRow AS pr
            JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype}
            JOIN TokoMultiStore AS tms ON {tms.multistorepricegroup} = {pr.ug}}
          where
            {pr.product} = {tvp.pk}
            AND {tms.name} = 'plallegromarketplace'
            AND {pt.code} = 'SPECIAL'
            AND {pr.minqtd} > 0
            AND (
              (
                {pr.startTime} <= CURRENT_TIMESTAMP
                AND {pr.endTime} >= CURRENT_TIMESTAMP
              )
              OR (
                {pr.startTime} IS NULL
                AND {pr.endTime} >= CURRENT_TIMESTAMP
              )
              OR (
                {pr.startTime} <= CURRENT_TIMESTAMP
                AND {pr.endTime} IS NULL
              )
              OR (
                {pr.startTime} IS NULL
                AND {pr.endTime} IS NULL
              )
            )
          GROUP BY
            {pr.product}}}
        ) THEN (
          {{
          SELECT
            MIN ({pr.price})
          FROM
            {PriceRow AS pr
            JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype}
            JOIN TokoMultiStore AS tms ON {tms.multistorepricegroup} = {pr.ug}}
          WHERE
            {pr.product} = {tvp.pk}
            AND {tms.name} = 'plallegromarketplace'
            AND {pt.code} = 'SPECIAL'
            AND {pr.minqtd} > 0
            AND (
              (
                {pr.startTime} <= CURRENT_TIMESTAMP
                AND {pr.endTime} >= CURRENT_TIMESTAMP
              )
              OR (
                {pr.startTime} IS NULL
                AND {pr.endTime} >= CURRENT_TIMESTAMP
              )
              OR (
                {pr.startTime} <= CURRENT_TIMESTAMP
                AND {pr.endTime} IS NULL
              )
              OR (
                {pr.startTime} IS NULL
                AND {pr.endTime} IS NULL
              )
            )
          GROUP BY
            {pr.product}}}
        ) ELSE (
          {{
          SELECT
            MIN ({pr.price})
          FROM
            {PriceRow AS pr
            JOIN PriceType AS pt ON {pt.pk} = {pr.pricetype}}
          WHERE
            {pr.product} = {tvp.pk}
            AND {pt.code} = 'RRP'
            AND {pr.minqtd} > 0
            AND (
              (
                {pr.startTime} <= CURRENT_TIMESTAMP
                AND {pr.endTime} >= CURRENT_TIMESTAMP
              )
              OR (
                {pr.startTime} IS NULL
                AND {pr.endTime} >= CURRENT_TIMESTAMP
              )
              OR (
                {pr.startTime} <= CURRENT_TIMESTAMP
                AND {pr.endTime} IS NULL
              )
              OR (
                {pr.startTime} IS NULL
                AND {pr.endTime} IS NULL
              )
            )
          GROUP BY
            {pr.product}}}
        ) END
      ) AS cost,
      (
        CASE WHEN EXISTS (
          {{
          SELECT
            {zdm.code}
          FROM
            { BaseSite2DeliveryModeRel AS rel
            JOIN CMSSite AS s ON {rel.source} = {s.pk}
            JOIN ZoneDeliveryMode AS zdm ON {rel.target} = {zdm.pk} }
          WHERE
            {s.uid} = 'plallegromarketplace' }}
        ) THEN (
          {{
          SELECT
            TOP 1 {zdm.code}
          FROM
            {ProductDeliveryModeRelation AS pdmr
            JOIN ZoneDeliveryMode AS zdm ON {pdmr.target} = {zdm.pk}
            LEFT JOIN BaseSite2DeliveryModeRel AS siteRel ON {zdm.pk} = {siteRel.target}
            LEFT JOIN CMSSite AS site ON {siteRel.source} = {site.pk}}
          WHERE
            {pdmr.source} = {tvp.pk}
            AND {site.uid} = 'plallegromarketplace' }}
        ) ELSE (
          {{
          SELECT
            TOP 1 {zdm.code}
          FROM
            {ProductDeliveryModeRelation AS pdmr
            JOIN ZoneDeliveryMode AS zdm ON {pdmr.target} = {zdm.pk}
            LEFT JOIN BaseStore2DeliveryModeRel AS storeRel ON {zdm.pk} = {storeRel.target}
            LEFT JOIN BaseStore AS store ON {storeRel.source} = {store.pk}}
          WHERE
            {pdmr.source} = {tvp.pk}
            AND {store.uid} = 'pl' }}
        ) END
      ) AS fulfillmentPolicyId,
      CONCAT(
        'Brand|Model|Color|Size|',
        (
          {{
          SELECT
            STRING_AGG(
              CONVERT(
                NVARCHAR(max),
                CONCAT(
                  {cc.name[pl_PL] : o}, '.', {ca.name[pl_PL] : o}
                )
              ),
              '|'
            )
          FROM
            { ProductFeature AS pf
            JOIN ClassAttributeAssignment AS caa ON {caa.pk} = {pf.classificationAttributeAssignment}
            JOIN ClassificationClass AS cc ON {cc.pk} = {caa.classificationClass}
            JOIN ClassificationAttribute AS ca ON {ca.pk} = {caa.classificationAttribute}
            JOIN ClassificationAttributeVisibilityEnum AS cave ON {cave.pk} = {caa.visibility} }
          WHERE
            {pf.product} = {tvp.pk}
            AND {cave.code} = 'visible' }}
        )
      ) AS aspect_names,
      CONCAT(
        'SAMSUNG|',
        {tvp.code},
        '|',
        CASE WHEN {swc.colorName} IS NULL THEN ' ' ELSE {swc.colorName} END,
        '|',
        CASE WHEN {tvp.size[pl_PL] : o} IS NULL THEN ' ' ELSE {tvp.size[pl_PL] : o} END,
        '|',
        (
          {{
          SELECT
            STRING_AGG(
              CONVERT(
                NVARCHAR(max),
                {pf.stringValue}
              ),
              '|'
            )
          FROM
            { ProductFeature AS pf
            JOIN ClassAttributeAssignment AS caa ON {caa.pk} = {pf.classificationAttributeAssignment}
            JOIN ClassificationClass AS cc ON {cc.pk} = {caa.classificationClass}
            JOIN ClassificationAttribute AS ca ON {ca.pk} = {caa.classificationAttribute}
            JOIN ClassificationAttributeVisibilityEnum AS cave ON {cave.pk} = {caa.visibility} }
          WHERE
            {pf.product} = {tvp.pk}
            AND {cave.code} = 'visible' }}
        )
      ) AS aspect_values,
      (
        CASE WHEN {tvp.pk} NOT IN (
          {{SELECT {mserel.target}
          FROM
            {TokoMultiStore2ExclusionProductRelation AS mserel
            JOIN TokoMultiStore AS tms ON {mserel.source} = {tms.pk}}
          WHERE
            {tms.name} = 'plallegromarketplace' }}
        )
        AND {base.pk} NOT IN (
          {{SELECT {mserel.target}
          FROM
            {TokoMultiStore2ExclusionProductRelation AS mserel
            JOIN TokoMultiStore AS tms ON {mserel.source} = {tms.pk}}
          WHERE
            {tms.name} = 'plallegromarketplace' }}
        )
        AND {catRel.source} NOT IN (
          {{SELECT {mserel.target}
          FROM
            {TokoMultiStore2ExclusionCategoryRelation AS mserel
            JOIN TokoMultiStore AS tms ON {mserel.source} = {tms.pk}}
          WHERE
            {tms.name} = 'plallegromarketplace' }}
        ) THEN 'Include' ELSE 'Exclude' END
      ) AS flag,
      (
        {{
        select
          (
            CASE WHEN {sl.productcode} IN (
              {{SELECT {slt.productcode}
              FROM
                {StockLevel AS slt
                JOIN InStockStatus AS isst ON {slt.instockstatus} = {isst.pk}
                and {isst.codeLowerCase} = 'forceinstock' }
              where
                {slt.productcode} = {tvp.code}
                and {slt.warehouse} in (
                  {{
                  select
                    {pk}
                  from
                    {Warehouse}
                  where
                    (
                      EXISTS (
                        {{
                        select
                          {rel.target}
                        from
                          { BaseSite2WarehouseRel as rel
                          join CMSSite as bs on {rel.source} = {bs.pk} }
                        where
                          {bs.uid} = 'plallegromarketplace' }}
                      )
                      and {pk} in (
                        {{
                        select
                          {rel.target}
                        from
                          { BaseSite2WarehouseRel as rel
                          join CMSSite as bs on {rel.source} = {bs.pk} }
                        where
                          {bs.uid} = 'plallegromarketplace' }}
                      )
                    )
                    or (
                      NOT EXISTS (
                        {{
                        select
                          {rel.target}
                        from
                          { BaseSite2WarehouseRel as rel
                          join CMSSite as bs on {rel.source} = {bs.pk} }
                        where
                          {bs.uid} = 'plallegromarketplace' }}
                      )
                      and {pk} in (
                        {{
                        select
                          {rel.target}
                        from
                          { BaseStore2WarehouseRel as rel
                          join BaseStore as bs on {rel.source} = {bs.pk} }
                        where
                          {bs.uid} = 'pl' }}
                      )
                    )
                    or (
                      EXISTS (
                        {{
                        select
                          {virtualWarehouse}
                        from
                          {CMSSite}
                        where
                          {uid} = 'plallegromarketplace'
                          and {virtualWarehouse} is not null}}
                      )
                      and {pk} in (
                        {{
                        select
                          {virtualWarehouse}
                        from
                          {CMSSite}
                        where
                          {uid} = 'plallegromarketplace'
                          and {virtualWarehouse} is not null}}
                      )
                    )
                    or (
                      NOT EXISTS (
                        {{
                        select
                          {virtualWarehouse}
                        from
                          {CMSSite}
                        where
                          {uid} = 'plallegromarketplace'
                          and {virtualWarehouse} is not null}}
                      )
                      and {pk} in (
                        {{
                        select
                          {virtualWarehouse}
                        from
                          {BaseStore}
                        where
                          {uid} = 'pl'
                          and {virtualWarehouse} is not null}}
                      )
                    ) }}
                ) }}
            )
            OR SUM(
              COALESCE({sl.available}, 0)
            ) - SUM(
              COALESCE({sl.reserved}, 0)
            ) - SUM(
              COALESCE({sl.safetyStock}, 0)
            ) < 0 THEN 0 ELSE sum(
              COALESCE({sl.available}, 0)
            ) - sum(
              COALESCE({sl.reserved}, 0)
            ) - sum(
              COALESCE({sl.safetyStock}, 0)
            ) END
          )
        from
          {StockLevel as sl
          left join InStockStatus as iss on {sl.instockstatus} = {iss.pk}
          and {iss.codeLowerCase} <> 'forceoutofstock' }
        where
          {sl.productcode} = {tvp.code}
          and {sl.warehouse} in (
            {{
            select
              {pk}
            from
              {Warehouse}
            where
              (
                EXISTS (
                  {{
                  select
                    {rel.target}
                  from
                    { BaseSite2WarehouseRel as rel
                    join CMSSite as bs on {rel.source} = {bs.pk} }
                  where
                    {bs.uid} = 'plallegromarketplace' }}
                )
                and {pk} in (
                  {{
                  select
                    {rel.target}
                  from
                    { BaseSite2WarehouseRel as rel
                    join CMSSite as bs on {rel.source} = {bs.pk} }
                  where
                    {bs.uid} = 'plallegromarketplace' }}
                )
              )
              or (
                NOT EXISTS (
                  {{
                  select
                    {rel.target}
                  from
                    { BaseSite2WarehouseRel as rel
                    join CMSSite as bs on {rel.source} = {bs.pk} }
                  where
                    {bs.uid} = 'plallegromarketplace' }}
                )
                and {pk} in (
                  {{
                  select
                    {rel.target}
                  from
                    { BaseStore2WarehouseRel as rel
                    join BaseStore as bs on {rel.source} = {bs.pk} }
                  where
                    {bs.uid} = 'pl' }}
                )
              )
              or (
                EXISTS (
                  {{
                  select
                    {virtualWarehouse}
                  from
                    {CMSSite}
                  where
                    {uid} = 'plallegromarketplace'
                    and {virtualWarehouse} is not null}}
                )
                and {pk} in (
                  {{
                  select
                    {virtualWarehouse}
                  from
                    {CMSSite}
                  where
                    {uid} = 'plallegromarketplace'
                    and {virtualWarehouse} is not null}}
                )
              )
              or (
                NOT EXISTS (
                  {{
                  select
                    {virtualWarehouse}
                  from
                    {CMSSite}
                  where
                    {uid} = 'plallegromarketplace'
                    and {virtualWarehouse} is not null}}
                )
                and {pk} in (
                  {{
                  select
                    {virtualWarehouse}
                  from
                    {BaseStore}
                  where
                    {uid} = 'pl'
                    and {virtualWarehouse} is not null}}
                )
              ) }}
          )
        group by
          {sl.productcode}}}
      ) AS shipToQuantity,
      CASE WHEN {base.name[pl_PL] : o} IS NULL THEN {base.code} ELSE {base.name[pl_PL] : o} END AS parent_sku_name,
      CASE WHEN {tvp.code} IN (
        'QE32LS03BBUXXH', 'QE65LS01BAUXXH',
        'QE65LS01BBUXXH', 'QE75LS03BAUXXH',
        'QE85LS03BAUXXH', 'QE43QN91BATXXH',
        'QE50QN91BATXXH', 'QE55QN700BTXXH',
        'QE55QN85BATXXH', 'QE55QN91BATXXH',
        'QE65QN700BTXXH', 'QE65QN800BTXXH',
        'QE65QN85BATXXH', 'QE65QN900BTXXH',
        'QE65QN91BATXXH', 'QE75QN700BTXXH',
        'QE75QN800BTXXH', 'QE75QN85BATXXH',
        'QE75QN900BTXXH', 'QE75QN91BATXXH',
        'QE85QN800BTXXH', 'QE85QN85BATXXH',
        'QE85QN900BTXXH', 'QE85QN90BATXXH',
        'QE43Q67BAUXXH', 'QE50Q67BAUXXH',
        'QE55Q67BAUXXH', 'QE55Q77BATXXH',
        'QE55Q80BATXXH', 'QE65Q67BAUXXH',
        'QE65Q77BATXXH', 'QE65Q80BATXXH',
        'QE75Q67BAUXXH', 'QE75Q77BATXXH',
        'QE75Q80BATXXH', 'QE85Q60BAUXXH',
        'QE85Q70BATXXH', 'QE85Q80BATXXH',
        'QE55S95BATXXH', 'QE65S95BATXXH',
        'QE55QN95BATXXH', 'QE65QN95BATXXH',
        'QE75QN95BATXXH', 'QE85QN95BATXXH',
        'QE43LS05BAUXXH', 'QE43LS05TCUXXH',
        'QE43LS01BAUXXH', 'QE43LS01BBUXXH',
        'QE43LS01TAUXXH', 'QE43LS01TBUXXH',
        'QE43Q67AAUXXH', 'QE50LS01BAUXXH',
        'QE50LS01BBUXXH', 'QE50LS01TAUXXH',
        'QE50LS01TBUXXH', 'QE55LS01BAUXXH',
        'QE55LS01BBUXXH', 'QE55LS01TBUXXH',
        'QE50Q67AAUXXH', 'QE55Q67AAUXXH',
        'QE55LS03AAUXXH', 'QE55LS03BAUXXH',
        'QE55Q77AATXXH', 'QE55Q80AATXXH',
        'QE55QN85AATXXH', 'QE55QN95AATXXH',
        'QE65Q67AAUXXH', 'QE65LS03AAUXXH',
        'QE65LS03BAUXXH', 'QE65QN800ATXXH',
        'QE65Q80AATXXH', 'QE65Q77AATXXH',
        'QE65QN85AATXXH', 'QE75Q67AAUXXH',
        'QE75LS03AAUXXH', 'QE75Q77AATXXH',
        'QE75QN800ATXXH', 'QE75QN85AATXXH',
        'QE85Q80AATXXH', 'QE75QN91AATXXH',
        'QE43LS03AAUXXH', 'QE43LS03BAUXXH',
        'QE50LS03AAUXXH', 'QE50LS03BAUXXH',
        'QE50QN91AATXXH', 'QE55LST7TCUXXH',
        'QE55QN91AATXXH', 'QE65QN900ATXXH',
        'QE65QN95AATXXH', 'QE75Q80AATXXH',
        'QE75QN900ATXXH', 'QE85LS03AAUXXH',
        'QE65LST7TCUXXH', 'QE75LST7TCUXXH',
        'QE55LS01TAUXXH', 'QE50Q80AATXXH',
        'QE85QN85AATXXH', 'QE65QN91AATXXH',
        'QE75QN95AATXXH', 'QE85QN95AATXXH',
        'QE43LS05TAUXXH', 'QE85QN800ATXXH',
        'QE85QN900ATXXH', 'QE85Q60AAUXXH',
        'QE85Q70AATXXH', 'QE85QN90AATXXH',
        'QE32LS03TCUXXH'
      ) THEN 'Qled' WHEN {tvp.code} IN (
        'UE43AU8002KXXH', 'UE50AU8002KXXH',
        'UE75AU8002KXXH', 'UE65AU8002KXXH',
        'UE32T4002AKXXH', 'UE32T4302AKXXH',
        'UE70AU8072UXXH', 'UE55AU8002KXXH',
        'UE85AU8002KXXH', 'UE70AU8002KXXH',
        'UE50TU7092UXXH', 'UE55TU7092UXXH',
        'UE65TU7092UXXH', 'UE75TU7092UXXH',
        'UE43AU7192UXXH', 'UE50AU7192UXXH',
        'UE55AU7192UXXH', 'UE65AU7192UXXH',
        'UE75AU7192UXXH', 'UE85AU7192UXXH',
        'UE43BU8002KXXH', 'UE50BU8002KXXH',
        'UE55BU8002KXXH', 'UE65BU8002KXXH',
        'UE75BU8002KXXH', 'UE85BU8002KXXH',
        'UE32T5302CKXXH'
      ) THEN 'LED' ELSE '' END AS tv_type,
      ROW_NUMBER () OVER (
        PARTITION BY {tvp.code},
        {tvp.baseProduct}
        ORDER BY
          {catg.code} DESC
      ) AS rowNum
    FROM
      {TokoVariantProduct AS tvp
      JOIN Product AS base ON {tvp.baseProduct} = {base.pk}
      JOIN CatalogVersion AS cv ON {tvp.catalogVersion} = {cv.pk}
      JOIN Catalog AS cat ON {cv.pk} = {cat.activeCatalogVersion}
      JOIN CategoryProductRelation AS catRel ON {catRel.target} = {base.pk}
      JOIN Category AS catg ON {catRel.source} = {catg.pk}
      JOIN CategoryType AS catype ON {catg.categoryType} = {catype.pk}
      JOIN ArticleApprovalStatus AS aas ON {tvp.approvalStatus} = {aas.pk}
      JOIN CategoryCategoryRelation AS ccr ON {ccr.target} = {catg.pk}
      JOIN Category AS catg2 ON {ccr.source} = {catg2.pk}
      JOIN CategoryType AS catype2 ON {catg2.categoryType} = {catype2.pk}
      JOIN ProductType AS ptype ON {ptype.pk} = {tvp.productType}
      LEFT JOIN SwatchColor AS swc ON {tvp.color} = {swc.pk}
      LEFT JOIN CategoryCategoryRelation AS ccr2 ON {ccr2.target} = {catg2.pk}
      LEFT JOIN Category AS catg3 ON {ccr2.source} = {catg3.pk}}
    WHERE
      {aas.code} = 'approved'
      AND {cat.id} = 'plallegromarketplaceCatalog'
      AND {cv.version} = 'Online'
      AND {catype.code} = 'NAV'
      AND {catype2.code} = 'NAV'
      AND {ptype.code} = 'NORMAL' }}
  ) AS temp
WHERE
        rowNum = 1




