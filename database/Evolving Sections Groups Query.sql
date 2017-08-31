WITH school_map AS (SELECT sch.site_id
, CASE WHEN sch.site_id = 6097729 THEN 'BAM'
	WHEN sch.site_id = 131177 THEN 'BHS' 
    WHEN sch.site_id = 134924 THEN 'BTA'
    WHEN sch.site_id = 122804 THEN 'Pre'
    WHEN sch.site_id = 6090195 THEN 'Crag'
    WHEN sch.site_id = 6090211 THEN 'Emer'
    WHEN sch.site_id = 6090252 THEN 'Jeff'
    WHEN sch.site_id = 6105316 THEN 'JM'
    WHEN sch.site_id = 6090278 THEN 'LeCo'
    WHEN sch.site_id = 6090294 THEN 'Long'
    WHEN sch.site_id = 6090286 THEN 'MX'
    WHEN sch.site_id = 6056857 THEN 'King'
    WHEN sch.site_id = 6504 THEN 'NPS'
    WHEN sch.site_id = 6090302 THEN 'Oxfo'
    WHEN sch.site_id = 6090187 THEN 'Rosa'
    WHEN sch.site_id = 6090310 THEN 'TO'
    WHEN sch.site_id = 6090328 THEN 'Wash'
    WHEN sch.site_id = 6056865 THEN 'Will'
    WHEN sch.site_id = 9999999 THEN 'DO'
    WHEN sch.site_id = 999999999 THEN 'CES'
    WHEN sch.site_id = 10000000 THEN 'PVTMS'
    WHEN sch.site_id = 20000000 THEN 'Inact'
    WHEN sch.site_id = 999999 THEN 'Grad'
    ELSE NULL
    END SiteShortName
, sch.site_name
FROM sites sch)


SELECT DISTINCT SECTIONID
, SCHOOLID
, GROUP_EMAIL
, GROUP_NAME
, GROUP_DESCRIPTION
, GROUP_OWNER
, COURSE_NUMBER
, COURSE_NAME
, 1 AS TERMID

FROM (SELECT st.section_id AS SECTIONID
, sch.site_id AS SCHOOLID

-- , REGEXP_REPLACE(LOWER(tb.timeblock_name), '\(.+?\)\s*|^period|6/7/8| ', '','g') period_name

, LOWER('Z-'||REGEXP_REPLACE(u.last_name, '\W+', '','g')||'-'
    ||SUBSTR(u.first_name, 1, 1)
    ||CASE WHEN gl.short_name IN ('6','7','8','9','10','11','12') 
        THEN '-P'||REGEXP_REPLACE(LOWER(tb.timeblock_name), '\(.+?\)\s*|^period|6/7/8| ', '','g') 
        ELSE '' 
    END
    ||'-'||CASE WHEN COUNT(*) OVER (PARTITION BY st.section_id) > 1 THEN 'Y-' ELSE SUBSTR(tr.term_name, 1,2)||'-' END
    ||sm.SiteShortName||'-1718')||'@berkeley.net' AS GROUP_EMAIL

, 'Z-'||REGEXP_REPLACE(u.last_name, '\W+', '','g')||' '
    ||SUBSTR(u.first_name, 1, 1)
    ||CASE WHEN gl.short_name IN ('6','7','8','9','10','11','12') 
        THEN ' P'||REGEXP_REPLACE(LOWER(tb.timeblock_name), '\(.+?\)\s*|^period|6/7/8| ', '','g') 
        ELSE ''
    END
    ||' '||CASE WHEN COUNT(*) OVER (PARTITION BY st.section_id) > 1 THEN 'Y ' ELSE SUBSTR(tr.term_name, 1,2)||' ' END
    ||sm.SiteShortName||' 17/18' AS GROUP_NAME

, sm.site_name||' '||u.Last_Name||' '||SUBSTR(u.first_name, 1, 1)||' '
  ||CASE WHEN gl.short_name IN ('TK', 'K', '1', '2', '3', '4', '5') THEN crs.short_name
    ELSE 
        CASE WHEN COUNT(*) OVER (PARTITION BY st.section_id) > 1 
            THEN 'Y' 
            ELSE SUBSTR(tr.term_name, 1,2) 
        END
    ||' Period '||REGEXP_REPLACE(LOWER(tb.timeblock_name), '\(.+?\)\s*|^period|6/7/8| ', '','g') 
    END GROUP_DESCRIPTION

, tr.term_id AS TERMID
, u.user_id AS GROUP_OWNER
-- , u.local_user_id
-- , u.last_name
-- , u.first_name
, crs.school_course_id AS COURSE_NUMBER
, st.section_id AS SECTION_NUMBER

, crs.short_name AS COURSE_NAME
--, tb.timeblock_name Period
--, CASE WHEN COUNT(*) OVER (PARTITION BY st.section_id) > 1 THEN 'Y' ELSE SUBSTR(tr.term_name, 1,2) END
, dense_rank() OVER (PARTITION BY st.section_id ORDER BY u.last_name, crs.short_name, tb.timeblock_name) RNK

FROM  section_teacher_aff st
	JOIN section_term_aff sta
    	ON st.section_id = sta.section_id
    JOIN terms tr
    	ON tr.term_id = sta.term_id
    JOIN sessions ses
    	ON ses.session_id = tr.session_id
	JOIN sites sch
    	ON sch.site_id = ses.site_id
    LEFT OUTER JOIN grade_levels gl
		ON gl.grade_level_id = sch.start_grade_level_id
	JOIN section_course_aff sca
		ON sca.section_id = st.section_id
	JOIN courses crs
		ON crs.course_id = sca.course_id
    JOIN section_timeblock_aff stb
    	ON stb.section_id = st.section_id
    JOIN timeblocks tb
    	ON tb.timeblock_id = stb.timeblock_id
	JOIN users u
		ON u.user_id = st.user_id
    JOIN school_map AS sm
        ON sch.site_id = sm.site_id

WHERE ses.academic_year = 2018
AND st.primary_teacher = True
AND gl.short_name IN ('TK','K','1','2','3','4','5','6','7','8','9','10','11','12')
AND sch.exclude_from_state_reporting = '0'

-- AND u.user_id = 43
AND st.section_id IN (144701, 144513, 114029, 117441)
AND st.section_id != 112037 -- Dana Moran's Duplicate Section

ORDER BY group_email
) AS ranked_groups
WHERE RNK = 1;