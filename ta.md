-- ============================================================
-- List CM8 search templates and entry templates with the
-- users/groups from their bound ACLs.
--
-- Before running:
--   1. Run the ComponentTypeID lookup query for 'ICMSearch' and
--      'ICMEntryTemplate' separately, then replace the
--      ICMUT00303001 placeholder below with the correct
--      ICMUT<ComponentTypeID>001 table for each item type
--      (they will be different tables).
--   2. Validate table/column names against SYSCAT.TABLES /
--      SYSCAT.COLUMNS on your library server, since exact
--      names can shift slightly between CM8 versions.
--   3. Test in a non-production library server first.
-- ============================================================

SELECT
    it.ItemTypeName            AS TemplateType,      -- 'ICMSearch' or 'ICMEntryTemplate'
    i.ItemID                   AS TemplateItemID,
    tbl.Title                  AS TemplateName,       -- from the item-type's own attribute table
    i.ACLCode,
    al.PrivSetCode,
    ps.Type                    AS PrivSetKind,
    u.UserID,
    u.UserName,
    CASE u.UserKind
        WHEN 1 THEN 'GROUP'
        WHEN 0 THEN 'USER'
    END                         AS EntryKind
FROM
    ICMSTITEMS       i
INNER JOIN
    ICMSTITEMTYPEDEFS it
    ON it.ItemTypeID = i.ItemTypeID
INNER JOIN
    ICMSTACCESSLISTS  al
    ON al.ACLCode = i.ACLCode
INNER JOIN
    ICMSTUSERS        u
    ON u.UNum = al.UNum
LEFT JOIN
    ICMSTPRIVSETCODES ps
    ON ps.PrivSetCode = al.PrivSetCode
LEFT JOIN
    ICMUT00303001    tbl        -- placeholder: swap for the real ICMUT<CompTypeID>001 table
    ON tbl.ItemID = i.ItemID
WHERE
    it.ItemTypeName IN ('ICMSearch', 'ICMEntryTemplate')
ORDER BY
    it.ItemTypeName,
    TemplateName,
    EntryKind DESC
FOR READ ONLY WITH UR;