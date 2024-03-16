--metadb:items_with_requests

DROP FUNCTION IF EXISTS items_with_requests;

CREATE FUNCTION items_with_requests(
    earliest_request_date date DEFAULT '1900-01-01',
    latest_request_date date DEFAULT '2050-01-01'
)
RETURNS TABLE(
    barcode TEXT,
    index_title TEXT,
    item_id UUID,
    request_type TEXT,
    request_level TEXT,
    request_date DATE,
    temp_loc TEXT,
    username TEXT)
AS $$
	SELECT it.barcode,
	inst.index_title,
	rt.item_id,
	rt.request_type,
	rt.request_level,
	rt.request_date,
	lt.code AS temp_loc,
	usr.username
	FROM folio_circulation.request__t AS rt
	LEFT JOIN folio_inventory.item__t AS it ON rt.item_id = it.id
	LEFT JOIN folio_inventory.holdings_record__t AS hrt ON hrt.id = it.holdings_record_id
	LEFT JOIN folio_inventory.instance__t AS inst ON inst.id = hrt.instance_id
	LEFT JOIN folio_inventory.location__t AS lt ON lt.id = it.temporary_location_id 
	LEFT JOIN folio_users.users__t AS usr ON rt.requester_id = usr.id
	WHERE rt.status ILIKE 'open%'
	AND earliest_request_date <= request_date AND request_date < latest_request_date
	AND lt.code IN ('SUL-TS-PROCESSING', 'SUL-TS-COLLECTIONCARE')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;