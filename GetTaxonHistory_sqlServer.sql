USE [ICTVonline40]
GO

/****** Object:  StoredProcedure [dbo].[getTaxonHistory]    Script Date: 7/18/2025 5:08:30 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[getTaxonHistory]
	@currentMSL AS INT,
	@ictvID AS INT,
	@msl AS INT,
	@taxNodeID AS INT,
	@taxonName AS NVARCHAR(300),
	@vmrID AS INT
AS
BEGIN
	SET XACT_ABORT, NOCOUNT ON

	SET @taxonName = TRIM(@taxonName)

	--===========================================================================================================================================
	-- If taxnode_id wasn't provided, use one of the other parameters to lookup an associated taxnode_id. 
	-- Prioritize parameters as follows: taxnode_id, ictv_iv, vmr_id, taxon_name.
	--===========================================================================================================================================
	IF @taxNodeID IS NULL OR @taxNodeID < 1
	BEGIN 
		IF @ictvID IS NOT NULL AND @ictvID > 0
			BEGIN
				-- Find the most recent taxnode_id associated with the ictv_id.
				SELECT TOP 1 @taxNodeID = tn.taxnode_id
				FROM taxonomy_node_names tn
				WHERE tn.ictv_id = @ictvID
				AND (@msl IS NULL OR (@msl IS NOT NULL AND tn.msl_release_num = @msl))
				ORDER BY tn.msl_release_num DESC
			END
		ELSE IF @vmrID IS NOT NULL AND @vmrID > 0
			BEGIN
				-- Find the most appropriate taxnode_id associated with the VMR ID.
				SELECT TOP 1 @taxNodeID = si.taxnode_id
				FROM species_isolates si
				WHERE si.isolate_id = @vmrID
				ORDER BY si.isolate_sort ASC
			END
		ELSE IF @taxonName IS NOT NULL AND LEN(@taxonName) > 0
			BEGIN
				-- Find the most recent taxnode_id associated with the taxon_name.
				SELECT TOP 1 @taxNodeID = tn.taxnode_id
				FROM taxonomy_node_names tn
				WHERE tn.name = @taxonName
				ORDER BY tn.msl_release_num DESC
			END
		ELSE RAISERROR('Either taxnode_id, ictv_id, vmr_id, or taxon_name must be provided', 18, 1)
	END 

	-- Did we get a valid taxnode_id?
	IF @taxNodeID IS NULL OR @taxNodeID < 1 RAISERROR('Unable to determine taxnode_id', 18, 1)
		

	--===========================================================================================================================================
	-- Populate a table-valued variable with all historical versions of the taxon specified by the taxnode_id.
	--===========================================================================================================================================
	DECLARE @taxonChanges AS dbo.TaxonChangesTableType 
	INSERT INTO @taxonChanges (
		ictv_id,
		is_deleted,
		is_demoted,
		is_lineage_updated,
		is_merged,
		is_moved,
		is_new,
		is_promoted,
		is_renamed,
		is_split,
		left_idx,
		lineage,
		lineage_ids,
		modifications,
		msl_release_num,
		[name],
		prev_notes,
		prev_proposal,
		rank_names,
		taxnode_id,
		tree_id
	)
	SELECT 
		ictv_id, 
		is_deleted, 
		is_demoted = MAX(is_demoted), 
		is_lineage_updated = MAX(is_lineage_updated), 
		is_merged = MAX(is_merged), 
		is_moved = MAX(is_moved), 
		is_new = MAX(is_new), 
		is_promoted = MAX(is_promoted), 
		is_renamed = MAX(is_renamed), 
		is_split = MAX(is_split), 
		left_idx, 
		lineage, 
		lineage_ids, 
		modifications, 
		msl_release_num, 
		[name], 
		prev_notes, 
		prev_proposals = STRING_AGG(prev_proposal, ';'),
		rank_names, 
		taxnode_id, 
		tree_id 
	FROM (
		SELECT 
			ictv_id = node.ictv_id,
			is_deleted = MAX(prev_delta.is_deleted),
			is_demoted = MAX(prev_delta.is_demoted),
			is_lineage_updated = MAX(prev_delta.is_lineage_updated),
			is_merged = MAX(prev_delta.is_merged),
			is_moved = MAX(prev_delta.is_moved),
			is_new = MAX(prev_delta.is_new),
			is_promoted = MAX(prev_delta.is_promoted),
			is_renamed = MAX(prev_delta.is_renamed),
			is_split = MAX(prev_delta.is_split),
			node.left_idx,
			node.lineage,
			lineage_ids = (
				CASE WHEN node.realm_id IS NOT NULL THEN CAST(node.realm_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.subrealm_id IS NOT NULL THEN CAST(node.subrealm_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.kingdom_id IS NOT NULL THEN CAST(node.kingdom_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.subkingdom_id IS NOT NULL THEN CAST(node.subkingdom_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.phylum_id IS NOT NULL THEN CAST(node.phylum_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.subphylum_id IS NOT NULL THEN CAST(node.subphylum_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.class_id IS NOT NULL THEN CAST(node.class_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.subclass_id IS NOT NULL THEN CAST(node.subclass_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.order_id IS NOT NULL THEN CAST(node.order_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.suborder_id IS NOT NULL THEN CAST(node.suborder_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.family_id IS NOT NULL THEN CAST(node.family_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.subfamily_id IS NOT NULL THEN CAST(node.subfamily_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.genus_id IS NOT NULL THEN CAST(node.genus_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.subgenus_id IS NOT NULL THEN CAST(node.subgenus_id AS VARCHAR(12))+ ';' ELSE '' END + 
				CASE WHEN node.species_id IS NOT NULL THEN CAST(node.species_id AS VARCHAR(12))+ ';' ELSE '' END	
			),
			modifications = SUM( 
				prev_delta.is_deleted |
				prev_delta.is_demoted |
				prev_delta.is_lineage_updated |
				prev_delta.is_merged |
				prev_delta.is_moved |
				prev_delta.is_new |
				prev_delta.is_promoted |   
				prev_delta.is_renamed |   
				prev_delta.is_split
			),
			msl_release_num = CASE
				WHEN MAX(prev_delta.is_deleted) = 1 THEN node.msl_release_num + 1
				ELSE node.msl_release_num
			END,
			node.name,
			prev_notes= MAX(prev_delta.notes), 
			prev_proposal = CASE
				WHEN prev_delta.proposal IS NOT NULL THEN prev_delta.proposal 
				WHEN prev_delta.tag_csv2 <> '' THEN (
					SELECT TOP 1 d.proposal 
					FROM taxonomy_node_delta d  
					JOIN taxonomy_node t ON d.new_taxid = t.taxnode_id 
					WHERE node.left_idx > t.left_idx 
					AND node.right_idx < t.right_idx  
					AND node.tree_id = t.tree_id 
					AND t.level_id > 100    
					AND d.proposal IS NOT NULL
					ORDER BY t.level_id DESC 
				) 
			END,
			prev_delta.proposal,
			rank_names = (
				CASE WHEN node.realm_id IS NOT NULL THEN 'Realm;' ELSE '' END + 
				CASE WHEN node.subrealm_id IS NOT NULL THEN 'Subrealm;' ELSE '' END + 
				CASE WHEN node.kingdom_id IS NOT NULL THEN 'Kingdom;' ELSE '' END + 
				CASE WHEN node.subkingdom_id IS NOT NULL THEN 'Subkingdom;' ELSE '' END + 
				CASE WHEN node.phylum_id IS NOT NULL THEN 'Phylum;' ELSE '' END + 
				CASE WHEN node.subphylum_id IS NOT NULL THEN 'Subphylum;' ELSE '' END + 
				CASE WHEN node.class_id IS NOT NULL THEN 'Class;' ELSE '' END + 
				CASE WHEN node.subclass_id IS NOT NULL THEN 'Subclass;' ELSE '' END + 
				CASE WHEN node.order_id IS NOT NULL THEN 'Order;' ELSE '' END + 
				CASE WHEN node.suborder_id IS NOT NULL THEN 'Suborder;' ELSE '' END + 
				CASE WHEN node.family_id IS NOT NULL THEN 'Family;' ELSE '' END + 
				CASE WHEN node.subfamily_id IS NOT NULL THEN 'Subfamily;' ELSE '' END + 
				CASE WHEN node.genus_id IS NOT NULL THEN 'Genus;' ELSE '' END + 
				CASE WHEN node.subgenus_id IS NOT NULL THEN 'Subgenus;' ELSE '' END + 
				CASE WHEN node.species_id IS NOT NULL THEN 'Species;' ELSE '' END 
			),
			taxnode_id = node.taxnode_id,  
			tree_id = node.tree_id 

		FROM taxonomy_node_x AS node  
		LEFT JOIN taxonomy_node_delta AS prev_delta ON (
			prev_delta.new_taxid = node.taxnode_id

			-- This condition includes abolished taxa.
			OR (prev_delta.is_deleted = 1 AND prev_delta.prev_taxid = node.taxnode_id)
		)
		WHERE node.tree_id >= 19000000
		AND node.msl_release_num <= @currentMSL 
		AND node.target_taxnode_id = @taxNodeID

		GROUP BY node.msl_release_num, node.taxnode_id, node.tree_id, node.name, node.ictv_id, 
			node.lineage, node.left_idx, node.right_idx,
			prev_delta.notes, prev_delta.proposal, prev_delta.tag_csv2,
			node.realm_id, node.subrealm_id, node.kingdom_id, node.subkingdom_id, node.phylum_id, node.subphylum_id,
			node.class_id, node.subclass_id, node.order_id, node.suborder_id, node.family_id, node.subfamily_id,
			node.genus_id, node.subgenus_id, node.species_id

	) historyData

	GROUP BY ictv_id, is_deleted, left_idx, lineage, lineage_ids, modifications, msl_release_num , name, prev_notes, rank_names, taxnode_id, tree_id

	ORDER BY CASE
		-- If a taxon is abolished, it will be displayed in the next release.
		WHEN MAX(is_deleted) = 1 THEN msl_release_num + 1
		ELSE msl_release_num
	END DESC


	--===========================================================================================================================================
	-- Populate a table-valued variable with the release numbers of all MSL releases where the taxon has been changed.
	--===========================================================================================================================================
	DECLARE @modifiedReleases AS dbo.SingleIntTableType 
	INSERT INTO @modifiedReleases 
	SELECT DISTINCT msl_release_num
	FROM (
		SELECT msl_release_num, SUM(modifications) AS mods 
		FROM @taxonChanges
		GROUP BY msl_release_num

		-- Only include the current release if there are taxa associated with it. If the taxon was
		-- abolished in an MSL < (current release - 1) then the current release won't be included. 
		UNION ALL (
			SELECT CASE
				WHEN EXISTS (
					SELECT 1
					FROM @taxonChanges currentTaxa
					WHERE currentTaxa.msl_release_num = @currentMSL
				) THEN @currentMSL
				ELSE NULL
			END, 1
		)
	) abolished
	WHERE mods > 0
	AND msl_release_num IS NOT NULL


	--===========================================================================================================================================
	-- Get details of all MSL releases where the taxon has been changed.
	--===========================================================================================================================================
	SELECT
		rank_names = (
			CASE WHEN realms > 0 THEN 'realm,' ELSE '' END +  
			CASE WHEN subrealms > 0 THEN 'subrealm,' ELSE '' END +  
			CASE WHEN kingdoms > 0 THEN 'kingdom,' ELSE '' END +  
			CASE WHEN subkingdoms > 0 THEN 'subkingdom,' ELSE '' END +  
			CASE WHEN phyla > 0 THEN 'phylum,' ELSE '' END +  
			CASE WHEN subphyla > 0 THEN 'subphylum,' ELSE '' END +  
			CASE WHEN classes > 0 THEN 'class,' ELSE '' END +  
			CASE WHEN subclasses > 0 THEN 'subclass,' ELSE '' END +  
			CASE WHEN orders > 0 THEN 'order,' ELSE '' END +  
			CASE WHEN suborders > 0 THEN 'suborder,' ELSE '' END +  
			CASE WHEN families > 0 THEN 'family,' ELSE '' END +  
			CASE WHEN subfamilies > 0 THEN 'subfamily,' ELSE '' END +  
			CASE WHEN genera > 0 THEN 'genus,' ELSE '' END +  
			CASE WHEN subgenera > 0 THEN 'subgenus,' ELSE '' END +  
			CASE WHEN species > 0 THEN 'species' ELSE '' END  
		), 
		release_num = msl.msl_release_num, 
		title = substring(msl.notes,1,255), 
		tree_id, 
		msl.year 

	FROM view_taxa_level_counts_by_release msl
	JOIN @modifiedReleases mr ON mr.id = msl.msl_release_num
	ORDER BY msl.msl_release_num DESC 


	--===========================================================================================================================================
	-- Get all the taxon's versions from MSL releases where there was a change.
	--===========================================================================================================================================
	SELECT
		tn.ictv_id,
		tn.is_deleted,
		is_demoted,
		is_lineage_updated,
		is_merged,
		is_moved,
		is_new,
		is_promoted,
		is_renamed,
		is_split,
		tn.lineage,
		lineage_ids,
		rank_names AS lineage_ranks,
		tn.msl_release_num,
		tn.[name],

		-- The taxon's lineage in the previous MSL release.
		previous_lineage = (
			CASE WHEN ISNULL(prev_tn.realm, '') <> '' THEN 'Realm:'+prev_tn.realm+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.subrealm, '') <> '' THEN 'Subrealm:'+prev_tn.subrealm+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.kingdom, '') <> '' THEN 'Kingdom:'+prev_tn.kingdom+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.subkingdom, '') <> '' THEN 'Subkingdom:'+prev_tn.subkingdom+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.phylum, '') <> '' THEN 'Phylum:'+prev_tn.phylum+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.subphylum, '') <> '' THEN 'Subphylum:'+prev_tn.subphylum+';' ELSE '' END +   
			CASE WHEN ISNULL(prev_tn.class, '') <> '' THEN 'Class:'+prev_tn.class+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.subclass, '') <> '' THEN 'Subclass:'+prev_tn.subclass+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.[order], '') <> '' THEN 'Order:'+prev_tn.[order]+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.suborder, '') <> '' THEN 'Suborder:'+prev_tn.suborder+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.family, '') <> '' THEN 'Family:'+prev_tn.family+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.subfamily, '') <> '' THEN 'Subfamily:'+prev_tn.subfamily+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.genus, '') <> '' THEN 'Genus:'+prev_tn.genus+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.subgenus, '') <> '' THEN 'Subgenus:'+prev_tn.subgenus+';' ELSE '' END +  
			CASE WHEN ISNULL(prev_tn.species, '') <> '' THEN 'Species:'+prev_tn.species+';' ELSE '' END  
		),

		-- Names of this taxon's antecedents from the previous release.
        previous_names = CASE
            WHEN tn.is_deleted = 0 AND (is_merged = 1 OR is_renamed = 1 OR is_split = 1) THEN (

                -- Format the previous names as a comma-delimited list.
                SELECT STUFF((
                    SELECT ', ' + tn_previous.name
                    FROM taxonomy_node tn_changed
                    JOIN taxonomy_node_merge_split ms_changed ON ms_changed.prev_ictv_id = tn_changed.ictv_id
                    JOIN taxonomy_node tn_previous ON tn_previous.ictv_id = ms_changed.next_ictv_id
					JOIN taxonomy_node_delta delta_previous ON (
						delta_previous.new_taxid = tn_changed.taxnode_id
						AND delta_previous.prev_taxid = tn_previous.taxnode_id
					)
                    WHERE tn_changed.taxnode_id = tn.taxnode_id
                    AND tn_previous.msl_release_num = (tn.msl_release_num - 1)
                    ORDER BY tn_previous.left_idx
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
                , 1, 2, '') 
            )
            ELSE NULL
        END,
		prev_notes,
		prev_proposal,
		tn.taxnode_id,
		tn.tree_id

	FROM @taxonChanges tn
	JOIN @modifiedReleases mr ON mr.id = tn.msl_release_num

	-- For demoted, moved, and promoted taxa, include the previous version for its lineage.
	LEFT JOIN taxonomy_node_names prev_tn ON (
		(tn.is_demoted = 1 OR tn.is_moved = 1 OR tn.is_promoted = 1)
		AND prev_tn.ictv_id = tn.ictv_id
		AND prev_tn.msl_release_num = tn.msl_release_num - 1
	)
	ORDER BY 

		-- Sort by release
		tn.msl_release_num DESC,

		-- Sort the name alphabetically
		tn.left_idx ASC,

		/* The order of changes is New, Abolished, Promoted, Demoted, Merged, Split, Moved, Lineage updated, Renamed, and Unchanged. */
		is_new DESC,
		tn.is_deleted DESC,
		is_promoted DESC,
		is_demoted DESC,
		is_merged DESC,
		is_split DESC,
		is_moved DESC, 
		is_lineage_updated DESC,
		is_renamed DESC


	--===========================================================================================================================================
	-- Get the selected taxon
	--===========================================================================================================================================
	SELECT TOP 1
		ictv_id = node.ictv_id,
		node.lineage,
		lineage_ids = (
			CASE WHEN node.realm_id IS NOT NULL THEN CAST(node.realm_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.subrealm_id IS NOT NULL THEN CAST(node.subrealm_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.kingdom_id IS NOT NULL THEN CAST(node.kingdom_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.subkingdom_id IS NOT NULL THEN CAST(node.subkingdom_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.phylum_id IS NOT NULL THEN CAST(node.phylum_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.subphylum_id IS NOT NULL THEN CAST(node.subphylum_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.class_id IS NOT NULL THEN CAST(node.class_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.subclass_id IS NOT NULL THEN CAST(node.subclass_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.order_id IS NOT NULL THEN CAST(node.order_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.suborder_id IS NOT NULL THEN CAST(node.suborder_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.family_id IS NOT NULL THEN CAST(node.family_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.subfamily_id IS NOT NULL THEN CAST(node.subfamily_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.genus_id IS NOT NULL THEN CAST(node.genus_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.subgenus_id IS NOT NULL THEN CAST(node.subgenus_id AS VARCHAR(12))+ ';' ELSE '' END + 
			CASE WHEN node.species_id IS NOT NULL THEN CAST(node.species_id AS VARCHAR(12))+ ';' ELSE '' END	
		),
		lineage_ranks = (
			CASE WHEN node.realm_id IS NOT NULL THEN 'Realm;' ELSE '' END + 
			CASE WHEN node.subrealm_id IS NOT NULL THEN 'Subrealm;' ELSE '' END + 
			CASE WHEN node.kingdom_id IS NOT NULL THEN 'Kingdom;' ELSE '' END + 
			CASE WHEN node.subkingdom_id IS NOT NULL THEN 'Subkingdom;' ELSE '' END + 
			CASE WHEN node.phylum_id IS NOT NULL THEN 'Phylum;' ELSE '' END + 
			CASE WHEN node.subphylum_id IS NOT NULL THEN 'Subphylum;' ELSE '' END + 
			CASE WHEN node.class_id IS NOT NULL THEN 'Class;' ELSE '' END + 
			CASE WHEN node.subclass_id IS NOT NULL THEN 'Subclass;' ELSE '' END + 
			CASE WHEN node.order_id IS NOT NULL THEN 'Order;' ELSE '' END + 
			CASE WHEN node.suborder_id IS NOT NULL THEN 'Suborder;' ELSE '' END + 
			CASE WHEN node.family_id IS NOT NULL THEN 'Family;' ELSE '' END + 
			CASE WHEN node.subfamily_id IS NOT NULL THEN 'Subfamily;' ELSE '' END + 
			CASE WHEN node.genus_id IS NOT NULL THEN 'Genus;' ELSE '' END + 
			CASE WHEN node.subgenus_id IS NOT NULL THEN 'Subgenus;' ELSE '' END + 
			CASE WHEN node.species_id IS NOT NULL THEN 'Species;' ELSE '' END 
		),
		msl_release_num = CASE
			WHEN prev_delta.is_deleted = 1 THEN node.msl_release_num + 1
			ELSE node.msl_release_num
		END,
		node.name,
		taxnode_id = node.taxnode_id,  
		tree_id = node.tree_id,
		[year] = tree.name

	FROM taxonomy_node AS node
	JOIN taxonomy_node AS tree ON (
		tree.taxnode_id = node.tree_id
		AND tree.level_id = 100 -- The level ID for a tree
	)
	LEFT JOIN taxonomy_node_delta AS prev_delta ON (
		prev_delta.new_taxid = node.taxnode_id
		OR (prev_delta.is_deleted = 1 AND prev_delta.prev_taxid = node.taxnode_id)
	)
	WHERE node.tree_id >= 19000000
	AND node.msl_release_num <= @currentMSL  
	AND node.taxnode_id = @taxNodeID

END
GO


