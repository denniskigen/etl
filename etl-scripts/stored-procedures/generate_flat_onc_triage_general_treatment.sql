DELIMITER $$
CREATE DEFINER=`etl_user`@`%` PROCEDURE `generate_flat_onc_triage_general_treatment_v1_0`(IN query_type VARCHAR(50), IN queue_number INT, IN queue_size INT, IN cycle_size INT)
BEGIN
    SET @primary_table := "flat_onc_triage_general_treatment";
    SET @query_type := query_type;
                     
    SET @total_rows_written := 0;
                    
    SET @encounter_types := "(141)";
    
    SET @start := NOW();
    SET @table_version := "flat_onc_triage_general_treatment_v1.0";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (SELECT MAX(max_date_created) FROM etl.flat_obs);

    CREATE TABLE IF NOT EXISTS flat_onc_triage_general_treatment (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id INT,
        encounter_id INT,
        encounter_type INT,
        location_id INT,
        location_name VARCHAR(100),
		    encounter_datetime DATETIME,
        cur_visit_type INT,
        person_name VARCHAR(255),
        age INT,
        gender CHAR(20),
        identifiers VARCHAR(255),
        cancer_type SMALLINT,
        other_cancer SMALLINT,
        type_of_sarcoma SMALLINT,
        type_of_GU_cancer SMALLINT,
        type_of_GI_cancer SMALLINT,
        head_and_neck_cancer SMALLINT,
        gynecologic_cancer SMALLINT,
        lymphoma_cancer SMALLINT,
        skin_cancer SMALLINT,
        breast_cancer SMALLINT,
        type_of_leukemia SMALLINT,
        PRIMARY KEY encounter_id (encounter_id),
        INDEX date_created (date_created)
    );

    IF (@query_type = "build") THEN
        SELECT 'BUILDING..........................................';
							
        SET @write_table := CONCAT('flat_onc_triage_general_treatment_temp_', queue_number);
        SET @queue_table := CONCAT('flat_onc_triage_general_treatment_build_queue_', queue_number);

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @queue_table, ' (SELECT * FROM flat_onc_triage_general_treatment_build_queue LIMIT ', queue_size, ');'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
    END IF;

    IF (@query_type = 'sync') THEN
        SELECT 'SYNCING.....................................';
        
        SET @write_table := 'flat_onc_triage_general_treatment';
        SET @queue_table := 'flat_onc_triage_general_treatment_sync_queue';

        CREATE TABLE IF NOT EXISTS flat_onc_triage_general_treatment_sync_queue (person_id INT PRIMARY KEY);
        
        SET @last_update := null;

        SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

        SELECT 'Finding patients in amrs.encounters...';
            REPLACE INTO flat_onc_triage_general_treatment_sync_queue
            (SELECT DISTINCT
                patient_id
              FROM
                amrs.encounter
              WHERE 
                date_changed > @last_update
            );

        SELECT 'Finding patients in flat_obs...';
            REPLACE INTO flat_onc_triage_general_treatment_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_lab_obs...';
            REPLACE INTO flat_onc_triage_general_treatment_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_lab_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_orders...';
            REPLACE INTO flat_onc_triage_general_treatment_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_orders
              WHERE
                max_date_created > @last_update
            );

            REPLACE INTO flat_onc_triage_general_treatment_sync_queue
            (SELECT 
                person_id
              FROM 
		            amrs.person 
	            WHERE 
                date_voided > @last_update
            );

            REPLACE INTO flat_onc_triage_general_treatment_sync_queue
            (SELECT 
                person_id
              FROM 
		            amrs.person 
	            WHERE 
                date_changed > @last_update
            );
    END IF;

    -- Remove test patients
    SET @dyn_sql := CONCAT('DELETE t1 FROM ', @queue_table, ' t1
        JOIN amrs.person_attribute t2 USING (person_id)
        WHERE t2.person_attribute_type_id = 28 AND value = "true" AND voided = 0');
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
                    
    SET @person_ids_count = 0;
    SET @dyn_sql=CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ', @queue_table); 
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SELECT @person_ids_count AS 'num of patients to update';

    SET @dyn_sql := CONCAT('DELETE t1 FROM ',@primary_table, ' t1 JOIN ', @queue_table,' t2 USING (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
 
    SET @person_ids_count = 0;
    SET @dyn_sql := CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ', @queue_table); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SELECT @person_ids_count AS 'num patients to update';

    SET @dyn_sql := CONCAT('DELETE t1 FROM ', @primary_table, ' t1 join ', @queue_table,' t2 USING (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                    
    SET @total_time = 0;
    SET @cycle_number = 0;
    
    SET @total_time = 0;
    SET @cycle_number = 0;
    
    WHILE @person_ids_count > 0 DO
        SET @loop_start_time := NOW();

        -- Create temporary table with a set of person ids             
        DROP TEMPORARY TABLE IF EXISTS flat_onc_triage_general_treatment_build_queue__0;

        SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_onc_triage_general_treatment_build_queue__0 (person_id INT PRIMARY KEY) (SELECT * FROM ', @queue_table, ' LIMIT ', cycle_size, ');');
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        DROP TEMPORARY TABLE IF EXISTS flat_onc_triage_general_treatment_0a;
        SET @dyn_sql := CONCAT(
            'CREATE TEMPORARY TABLE flat_onc_triage_general_treatment_0a
            (SELECT 
                t1.person_id,
                t1.encounter_id,
                t1.encounter_datetime,
				        t1.encounter_type,
                t1.location_id,
                t1.obs,
                t1.obs_datetimes,
                t2.orders
            FROM
                etl.flat_obs t1
                    JOIN
                flat_onc_triage_general_treatment_build_queue__0 t0 USING (person_id)
                    LEFT JOIN
                etl.flat_orders t2 USING (encounter_id)
            WHERE
                t1.encounter_type IN ', @encounter_types, ');');
                            
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        INSERT INTO flat_onc_triage_general_treatment_0a
        (SELECT
            t1.person_id,
            t1.encounter_id,
            t1.test_datetime,
            t1.encounter_type,
	          null,
            t1.obs,
            null,
            null
        FROM 
          etl.flat_lab_obs t1
              JOIN
          flat_onc_triage_general_treatment_build_queue__0 t0 USING (person_id)
        );

        DROP TEMPORARY TABLE IF EXISTS flat_onc_triage_general_treatment_0;
        CREATE TEMPORARY TABLE flat_onc_triage_general_treatment_0 (INDEX encounter_id (encounter_id), INDEX person_enc (person_id, encounter_datetime))
        (SELECT 
            * 
        FROM
          flat_onc_triage_general_treatment_0a
        ORDER BY
          person_id, DATE(encounter_datetime)
        );
                        
		SET @cur_visit_type := null;
        SET @cancer_type := null;
        SET @other_cancer := null;
        SET @type_of_sarcoma := null;
        SET @type_of_GU_cancer := null;
        SET @type_of_GI_cancer := null;
        SET @head_and_neck_cancer := null;
        SET @gynecologic_cancer := null;
        SET @lymphoma_cancer := null;
        SET @skin_cancer := null;
        SET @breast_cancer := null;
        SET @type_of_leukemia := null;

        DROP TEMPORARY TABLE IF EXISTS flat_onc_triage_general_treatment_1;

        CREATE TEMPORARY TABLE flat_onc_triage_general_treatment_1
        (SELECT 
              obs,
              @prev_id := @cur_id as prev_id,
              @cur_id := t1.person_id as cur_id,
              t1.person_id,
              t1.encounter_id,
              t1.encounter_type,
              t1.location_id,
              l.name AS location_name,
              t1.encounter_datetime,
              CASE
                  WHEN obs REGEXP '!!1839=1911!!' THEN @cur_visit_type := 1
                  WHEN obs REGEXP '!!1839=1246!!' THEN @cur_visit_type := 2
                  ELSE @cur_visit_type := null
              END AS cur_visit_type,
			        CONCAT(COALESCE(person_name.given_name, ''),
                ' ',
              COALESCE(person_name.middle_name, ''),
                ' ',
              COALESCE(person_name.family_name, '')) AS person_name,
              CASE
                WHEN TIMESTAMPDIFF(YEAR, p.birthdate, curdate()) > 0 THEN round(TIMESTAMPDIFF(YEAR, p.birthdate, curdate()), 0)
                ELSE ROUND(TIMESTAMPDIFF(MONTH, p.birthdate, curdate()) / 12, 2)
              END AS age,
              p.gender,
              GROUP_CONCAT(DISTINCT id.identifier SEPARATOR ', ') AS identifiers,
              CASE
                  WHEN obs REGEXP '!!7176=6485' then @cancer_type := 1
                  WHEN obs REGEXP '!!7176=6514' then @cancer_type := 2
                  WHEN obs REGEXP '!!7176=6520' then @cancer_type := 3
                  WHEN obs REGEXP '!!7176=6528' then @cancer_type := 4
                  WHEN obs REGEXP '!!7176=6536' then @cancer_type := 5
                  WHEN obs REGEXP '!!7176=6551' then @cancer_type := 6
                  WHEN obs REGEXP '!!7176=6540' then @cancer_type := 7
                  WHEN obs REGEXP '!!7176=6544' then @cancer_type := 8
                  WHEN obs REGEXP '!!7176=216' then @cancer_type := 9
                  WHEN obs REGEXP '!!7176=5622' then @cancer_type := 10
                  WHEN obs REGEXP '!!7176=10129' then @cancer_type := 11
                  WHEN obs REGEXP '!!7176=10130' then @cancer_type := 12
              END AS cancer_type,
              CASE
                  WHEN obs REGEXP '!!1915=' then @other_cancer := GetValues(obs, 1915)
              END AS other_cancer,
              CASE
                  WHEN obs REGEXP '!!9843=507' then @type_of_sarcoma := 1
                  WHEN obs REGEXP '!!9843=6486' then @type_of_sarcoma := 2
                  WHEN obs REGEXP '!!9843=6487' then @type_of_sarcoma := 3
                  WHEN obs REGEXP '!!9843=6488' then @type_of_sarcoma := 4
                  WHEN obs REGEXP '!!9843=6489' then @type_of_sarcoma := 5
                  WHEN obs REGEXP '!!9843=6490' then @type_of_sarcoma := 6
              END AS type_of_sarcoma,
              CASE
                  WHEN obs REGEXP '!!6514=6515' then @type_of_GU_cancer:= 1
                  WHEN obs REGEXP '!!6514=6516' then @type_of_GU_cancer:= 2
                  WHEN obs REGEXP '!!6514=6517' then @type_of_GU_cancer:= 3
                  WHEN obs REGEXP '!!6514=6518' then @type_of_GU_cancer:= 4
                  WHEN obs REGEXP '!!6514=6519' then @type_of_GU_cancer:= 5
                  WHEN obs REGEXP '!!6514=5622' then @type_of_GU_cancer:= 6
              END AS type_of_GU_cancer,
              CASE
                  WHEN obs REGEXP '!!6520=6521' then @type_of_GI_cancer := 1
                  WHEN obs REGEXP '!!6520=6522' then @type_of_GI_cancer := 2
                  WHEN obs REGEXP '!!6520=6523' then @type_of_GI_cancer := 3
                  WHEN obs REGEXP '!!6520=6524' then @type_of_GI_cancer := 4
                  WHEN obs REGEXP '!!6520=6525' then @type_of_GI_cancer := 5
                  WHEN obs REGEXP '!!6520=6526' then @type_of_GI_cancer := 6
                  WHEN obs REGEXP '!!6520=6527' then @type_of_GI_cancer := 7
                  WHEN obs REGEXP '!!6520=6568' then @type_of_GI_cancer := 8
                  WHEN obs REGEXP '!!6520=5622' then @type_of_GI_cancer := 9
              END AS type_of_GI_cancer,
              CASE
                  WHEN obs REGEXP '!!6528=6529' then @head_and_neck_cancer := 1
                  WHEN obs REGEXP '!!6528=6530' then @head_and_neck_cancer := 2
                  WHEN obs REGEXP '!!6528=6531' then @head_and_neck_cancer := 3
                  WHEN obs REGEXP '!!6528=6532' then @head_and_neck_cancer := 4
                  WHEN obs REGEXP '!!6528=6533' then @head_and_neck_cancer := 5
                  WHEN obs REGEXP '!!6528=6534' then @head_and_neck_cancer := 6
                  WHEN obs REGEXP '!!6528=5622' then @head_and_neck_cancer := 7
              END AS head_and_neck_cancer,
              CASE
                  WHEN obs REGEXP '!!6536=6537' then @gynecologic_cancer := 1
                  WHEN obs REGEXP '!!6536=6538' then @gynecologic_cancer := 2
                  WHEN obs REGEXP '!!6536=6539' then @gynecologic_cancer := 3
                  WHEN obs REGEXP '!!6536=5622' then @gynecologic_cancer := 4
              END AS gynecologic_cancer,
              CASE
                  WHEN obs REGEXP '!!6551=6553' then @lymphoma_cancer := 1
                  WHEN obs REGEXP '!!6551=6552' then @lymphoma_cancer := 2
                  WHEN obs REGEXP '!!6551=8423' then @lymphoma_cancer := 3
                  WHEN obs REGEXP '!!6551=5622' then @lymphoma_cancer := 4
              END AS lymphoma_cancer,
              CASE
                  WHEN obs REGEXP '!!6540=6541' then @skin_cancer := 1
                  WHEN obs REGEXP '!!6540=6542' then @skin_cancer := 2
                  WHEN obs REGEXP '!!6540=6543' then @skin_cancer := 3
                  WHEN obs REGEXP '!!6540=5622' then @skin_cancer := 4
              END AS skin_cancer,
              CASE
                  WHEN obs REGEXP '!!9841=6545' then @breast_cancer := 1
                  WHEN obs REGEXP '!!9841=9842' then @breast_cancer := 2
                  WHEN obs REGEXP '!!9841=5622' then @breast_cancer := 3
              END AS breast_cancer,
              CASE
                  WHEN obs REGEXP '!!9844=6547' then @type_of_leukemia := 1
                  WHEN obs REGEXP '!!9844=6548' then @type_of_leukemia := 2
                  WHEN obs REGEXP '!!9844=6549' then @type_of_leukemia := 3
                  WHEN obs REGEXP '!!9844=6550' then @type_of_leukemia := 4
                  WHEN obs REGEXP '!!9844=5622' then @type_of_leukemia := 5
              END AS type_of_leukemia
          FROM 
              flat_onc_triage_general_treatment_0 t1
			  JOIN 
                  amrs.person p using (person_id)
		      LEFT JOIN
		          amrs.location `l` ON l.location_id = t1.location_id
			  LEFT JOIN 
				  amrs.person_name `person_name` ON (t1.person_id = person_name.person_id
                  AND (person_name.voided IS NULL || person_name.voided = 0))
			  LEFT JOIN 
				  amrs.patient_identifier `id` ON (p.person_id = id.patient_id AND (id.voided IS NULL || id.voided = 0))
   	      ORDER BY person_id, date(encounter_datetime) DESC
  	    );

          SET @prev_id = null;
          SET @cur_id = null;
						
          ALTER TABLE flat_onc_triage_general_treatment_1 DROP prev_id, DROP cur_id;

	        SELECT 
              COUNT(*)
          INTO 
              @new_encounter_rows
          FROM
              flat_onc_triage_general_treatment_1;
                              
          SELECT @new_encounter_rows;
          SET @total_rows_written := @total_rows_written + @new_encounter_rows;
          SELECT @total_rows_written;

          SET @dyn_sql := CONCAT('REPLACE INTO ', @write_table, 										  
              '(SELECT
                    null,
                    person_id,
                    encounter_id,
                    encounter_type,
                    location_id,
					          location_name,
                    encounter_datetime,
                    cur_visit_type,
                    person_name,
                    age,
                    gender,
                    identifiers,
                    cancer_type,
                    other_cancer,
                    type_of_sarcoma,
                    type_of_GU_cancer,
                    type_of_GI_cancer,
                    head_and_neck_cancer,
                    gynecologic_cancer,
                    lymphoma_cancer,
                    skin_cancer,
                    breast_cancer,
                    type_of_leukemia
                FROM 
                    flat_onc_triage_general_treatment_1 t1
                        JOIN 
                    amrs.location t2 USING (location_id))'
          );

          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;


          SET @dyn_sql=CONCAT('DELETE t1 from ',@queue_table,' t1 JOIN flat_onc_triage_general_treatment_build_queue__0 t2 USING (person_id);'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                                
          SET @dyn_sql=CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ',@queue_table,';'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                    
          SET @cycle_length = TIMESTAMPDIFF(second,@loop_start_time,NOW());
          SET @total_time = @total_time + @cycle_length;
          SET @cycle_number = @cycle_number + 1;
          
          SET @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);

          SELECT 
            @person_ids_count AS 'persons remaining',
            @cycle_length AS 'Cycle time (s)',
            CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
            @remaining_time AS 'Est time remaining (min)';
    END WHILE;

    IF (@query_type = 'build') THEN
        SET @dyn_sql := CONCAT('drop table ', @queue_table, ';'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                        
        SET @total_rows_to_write := 0;
        SET @dyn_sql := CONCAT('SELECT COUNT(*) INTO @total_rows_to_write FROM ', @write_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
                                                
        SET @start_write := NOW();
        SELECT CONCAT(@start_write, ' : Writing ', @total_rows_to_write, ' to ', @primary_table);

        SET @dyn_sql := CONCAT('REPLACE INTO ', @primary_table, '(SELECT * FROM ', @write_table, ');');
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
						
        SET @finish_write := NOW();
        SET @time_to_write := TIMESTAMPDIFF(SECOND, @start_write, @finish_write);
        SELECT CONCAT(@finish_write, ' : Completed writing rows. Time to write to primary table: ', @time_to_write, ' seconds ');                        
        
        SET @dyn_sql := CONCAT('DROP TABLE ', @write_table, ';'); 
        PREPARE s1 FROM @dyn_sql;
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;      
    END IF;
						
    SET @ave_cycle_length := CEIL(@total_time / @cycle_number);
    SELECT CONCAT('Average Cycle Length: ', @ave_cycle_length, ' second(s)');
            
    SET @end := NOW();
    INSERT INTO etl.flat_log VALUES (@start, @last_date_created, @table_version, TIMESTAMPDIFF(SECOND, @start, @end));
    SELECT CONCAT(@table_version, ': Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');
END$$
DELIMITER ;
