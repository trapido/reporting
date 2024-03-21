--metadb:function receiving_history

-- Report pulls a list of receiving pieces, by location and date received, if enetered.

DROP FUNCTION IF EXISTS receiving_history;

CREATE FUNCTION receiving_history(
	earliest_date_received date DEFAULT '1000-01-01',
    latest_date_received date DEFAULT '3000-01-01',
    pieces_location_filter text DEFAULT '')
RETURNS TABLE(
	piece_id uuid,
    title text,
	caption text,
	chronology text,
	enumeration text,
	receiving_status text,
	expected_receipt_date date,
	received_date date,
	piece_format text,
	"comment" text,
	supplement text,
	discovery_suppress boolean,
	display_on_holding boolean,
	copy_number text,
	pol_id uuid,
	checkin_items boolean,
	po_line_number text,
	receipt_status text,
	receiving_note text,
	date_ordered date,
	po_id uuid,
	item_id uuid,
	location_id uuid,
	holding_id uuid,
    piece_location_id uuid,
    piece_location_name text,
    piece_location_code text,
    piece_location_source text
)
AS $$
SELECT
	p.id AS piece_id,
	pol.jsonb->>'titleOrPackage' AS title,
	p.caption,
	--p.display_summary 
	p.chronology,
	p.enumeration,
	p.receiving_status,
	p.receipt_date AS expected_receipt_date,
	(p.received_date)::date,
	p.format AS piece_format,
	p."comment",
	p.supplement,
	p.discovery_suppress,
	p.display_on_holding,
	p.copy_number,
	pol.id AS pol_id,
	(pol.jsonb ->>'checkinItems')::boolean AS checkin_items,
	pol.jsonb ->>'poLineNumber' AS po_line_number,
	pol.jsonb ->>'receiptStatus' AS receipt_status,
	pol.jsonb -> 'details' ->> 'receivingNote' AS receiving_note,
	(po.jsonb->>'dateOrdered')::date AS date_ordered,
	po.id AS po_id,
	p.item_id,
	p.location_id,
	p.holding_id,
	CASE
		WHEN p.location_id IS NOT NULL THEN p.location_id
		ELSE hrt.permanent_location_id
	END AS piece_location_id,
	CASE
		WHEN piece_location.name IS NOT NULL THEN piece_location.name
		ELSE holdings_location.name
	END AS piece_location_name,
	CASE
		WHEN piece_location.code IS NOT NULL THEN piece_location.code
		ELSE holdings_location.code
	END AS piece_location_code,
	CASE
		WHEN p.location_id IS NOT NULL THEN 'piece_location'
		ELSE 'piece_holdings_location'
	END AS piece_location_source
FROM
	folio_orders.pieces__t AS p
LEFT JOIN folio_orders.po_line AS pol ON
	pol.id = p.po_line_id
LEFT JOIN folio_orders.purchase_order AS po ON
	po.id = pol.purchaseorderid
LEFT JOIN folio_inventory.holdings_record__t AS hrt ON
	hrt.id = p.holding_id
LEFT JOIN folio_inventory.location__t AS piece_location ON
	piece_location.id = p.location_id
LEFT JOIN folio_inventory.location__t AS holdings_location ON
	holdings_location.id = hrt.permanent_location_id
WHERE earliest_date_received <= p.received_date AND p.received_date < latest_date_received
AND pieces_location_filter IN (CASE
    WHEN p.location_id IS NOT NULL THEN piece_location.name
    ELSE holdings_location.name
    END, '')
$$
LANGUAGE SQL;