--metadb:function create_shelflist

--Report pulls a shelf list, by location and call number

DROP FUNCTION IF EXISTS create_shelflist;

CREATE FUNCTION create_shelflist(
    start_date_cataloged DATE DEFAULT '1900-01-01',
    end_date_cataloged DATE DEFAULT '2050-01-01',
    library_filter TEXT DEFAULT '',
    holdings_permanent_location_filter TEXT DEFAULT '', 
    item_temporary_location_filter TEXT DEFAULT '', 
    item_effective_location_filter TEXT DEFAULT '',
    holdings_type_filter TEXT DEFAULT '',
    material_type_filter TEXT DEFAULT '',
    inst_discovery_suppress_filter BOOLEAN DEFAULT FALSE,
    hld_discovery_suppress_filter BOOLEAN DEFAULT FALSE,
    itm_discovery_suppress_filter BOOLEAN DEFAULT FALSE,
    call_number_type_filter TEXT DEFAULT '',
    call_number_filter TEXT DEFAULT '')
RETURNS TABLE(
    hrid TEXT,
    title TEXT,
    inst_discovery_suppress BOOLEAN,
    inst_status TEXT,
    cataloged_date DATE,
    hld_discovery_suppress BOOLEAN,
    enumeration TEXT,
    chronology TEXT,
    barcode TEXT,
    hld_permanent_location_name TEXT,
    item_effective_location_name TEXT,
    item_temp_location_name TEXT,
    "library" TEXT,
    holdings_type TEXT,
    material_type TEXT,
    call_number TEXT,
    call_number_type TEXT,
    loan_type TEXT,
    item_status TEXT,
    item_discovery_suppress BOOLEAN,
    effective_shelving_order TEXT
    ) AS
$$
SELECT
inst.hrid,
inst.title,
inst.discovery_suppress AS inst_discovery_suppress,
inst_status."name" AS inst_status,
(COALESCE (inst.cataloged_date, '1900-01-01'))::date AS cataloged_date,
hld.discovery_suppress AS hld_discovery_suppress,
COALESCE(i.jsonb->>'enumeration', '') AS enumeration,
COALESCE(i.jsonb->>'chronology', '') AS chronology,
i.jsonb->>'barcode' AS barcode,
pl."name" AS hld_permanent_location_name,
el."name" AS item_effective_location_name,
tl."name" AS item_temp_location_name,
perm_loclib.code AS "library",
hld_type.name AS holdings_type,
mat_type.name AS material_type,
hld.call_number,
call_num_type.name AS call_number_type,
loan_type.name AS loan_type,
(i.jsonb->'status'->>'name')::TEXT AS item_status,
(i.jsonb->>'discoverySuppress')::BOOLEAN AS item_discovery_suppress,
(i.jsonb->>'effectiveShelvingOrder')::TEXT AS effective_shelving_order
FROM folio_inventory.instance__t AS inst
JOIN folio_inventory.holdings_record__t AS hld 
ON hld.instance_id = inst.id
LEFT JOIN folio_inventory.holdings_type__t AS hld_type
ON hld_type.id = hld.holdings_type_id 
LEFT JOIN folio_inventory.call_number_type__t AS call_num_type
ON call_num_type.id = hld.call_number_type_id 
LEFT JOIN folio_inventory.item AS i
ON i.holdingsrecordid = hld.id
LEFT JOIN folio_inventory.material_type__t AS mat_type
ON mat_type.id = i.materialtypeid
LEFT JOIN folio_inventory.loan_type__t AS loan_type
ON loan_type.id = i.permanentloantypeid
LEFT JOIN folio_inventory.location__t AS el
ON el.id = i.effectivelocationid 
LEFT JOIN folio_inventory.location__t AS tl
ON tl.id = i.temporarylocationid 
LEFT JOIN folio_inventory.location__t AS pl
ON pl.id = hld.permanent_location_id
LEFT JOIN folio_inventory.loclibrary__t AS perm_loclib
ON perm_loclib.id=pl.library_id
LEFT JOIN folio_inventory.loclibrary__t AS temp_loclib
ON temp_loclib.id=tl.library_id
LEFT JOIN folio_inventory.instance_status__t AS inst_status
ON inst_status.id = inst.status_id
WHERE start_date_cataloged <= (COALESCE (inst.cataloged_date, '1900-01-01'))::date AND (COALESCE (inst.cataloged_date, '1900-01-01'))::date < end_date_cataloged AND
library_filter IN (perm_loclib.code, '') AND
pl.name ILIKE '%'|| holdings_permanent_location_filter  ||'%' AND
tl.name ILIKE '%'|| item_temporary_location_filter ||'%' AND
el.name ILIKE '%'|| item_effective_location_filter ||'%' AND
inst_discovery_suppress_filter = COALESCE(inst.discovery_suppress, FALSE) AND 
hld_discovery_suppress_filter = COALESCE(hld.discovery_suppress, FALSE) AND 
itm_discovery_suppress_filter = COALESCE((i.jsonb->>'discoverySuppress')::boolean, FALSE) AND 
holdings_type_filter IN (hld_type.name, '') AND 
material_type_filter IN (mat_type.name, '') AND 
call_number_type_filter IN (call_number_type.name, '') AND
hld.call_number ILIKE call_number_filter || '%'
ORDER BY i.jsonb->>'effectiveShelvingOrder' COLLATE "C"
$$
LANGUAGE SQL;