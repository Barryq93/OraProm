SELECT
    it.ItemTypeName            AS TemplateType,      -- 'ICMSearch' or 'ICMEntryTemplate'
    i.ItemID                   AS TemplateItemID,
    tbl.Title                  AS TemplateName,       -- from the item-type's own attribute table, see note below
    i.ACLCode,
    al.PrivSetCode,
    ps.Type                    AS PrivSetKind,
    u.UserID,
    u.UserName,
    CASE u.UserKind
        WHEN 1 THEN 'GROUP'
        WHEN 0 THEN 'USER'
    END                         AS EntryKind
FROM ICMSTITEMS       i
JOIN ICMSTITEMTYPEDEFS it ON it.ItemTypeID = i.ItemTypeID
JOIN ICMSTACCESSLISTS  al ON al.ACLCode    = i.ACLCode
JOIN ICMSTUSERS        u  ON u.UNum        = al.UNum
LEFT JOIN ICMSTPRIVSETCODES ps ON ps.PrivSetCode = al.PrivSetCode
LEFT JOIN ICMUT00303001    tbl ON tbl.ItemID = i.ItemID   -- replace with the real ICMUT<CompTypeID>001 table for your item type
WHERE it.ItemTypeName IN ('ICMSearch', 'ICMEntryTemplate')
ORDER BY it.ItemTypeName, TemplateName, EntryKind DESC
FOR READ ONLY WITH UR;