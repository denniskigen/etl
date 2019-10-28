CREATE DEFINER=`etl_user`@`%` PROCEDURE `generate_flat_onc_htn_dm_triage_v1_0`(IN query_type VARCHAR(50), IN queue_number INT, IN queue_size INT, IN cycle_size INT)
BEGIN
    SET @primary_table := "flat_onc_htn_dm_triage";
    SET @query_type := query_type;
                     
    SET @total_rows_written := 0;
                    
    SET @encounter_types := "(130)";
    
    SET @start := now();
    SET @table_version := "flat_onc_htn_dm_triage_v1.0";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (SELECT MAX(max_date_created) FROM etl.flat_obs);

    CREATE TABLE IF NOT EXISTS flat_onc_htn_dm_triage (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id INT,
        encounter_id INT,
        encounter_type INT,
        location_id INT,
        location_name VARCHAR(100),
		    encounter_datetime DATETIME,
        cur_visit_type INT,
        age INT,
        gender CHAR(20),
        temperature DECIMAL(3, 1),
        waist_circumference DECIMAL(3, 1),
        weight DECIMAL(3, 1),
        height DECIMAL(3, 1),
        bmi DECIMAL(2, 1),
        systolic_bp INT,
        diastolic_bp INT,
        fasting_blood_glucose DECIMAL(3, 1),
        random_blood_glucose DECIMAL(3, 1),
        PRIMARY KEY encounter_id (encounter_id),
        INDEX date_created (date_created)
    );

    IF (@query_type = "build") THEN
        SELECT 'BUILDING..........................................';
							
        SET @write_table := CONCAT('flat_onc_htn_dm_triage_temp_', queue_number);
        SET @queue_table := CONCAT('flat_onc_htn_dm_triage_build_queue_', queue_number);

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @queue_table, ' (SELECT * FROM flat_onc_htn_dm_triage_build_queue LIMIT ', queue_size, ');'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
    END IF;

    IF (@query_type = 'sync') THEN
        SELECT 'SYNCING.....................................';
        
        SET @write_table := 'flat_onc_htn_dm_triage';
        SET @queue_table := 'flat_onc_htn_dm_triage_sync_queue';

        CREATE TABLE IF NOT EXISTS flat_onc_htn_dm_triage_sync_queue (person_id INT PRIMARY KEY);
        
        SET @last_update := null;

        SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

        SELECT 'Finding patients in amrs.encounters...';
            REPLACE INTO flat_onc_htn_dm_triage_sync_queue
            (SELECT DISTINCT
                patient_id
              FROM
                amrs.encounter
              WHERE 
                date_changed > @last_update
            );

        SELECT 'Finding patients in flat_obs...';
            REPLACE INTO flat_onc_htn_dm_triage_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_lab_obs...';
            REPLACE INTO flat_onc_htn_dm_triage_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_lab_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_orders...';
            REPLACE INTO flat_onc_htn_dm_triage_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_orders
              WHERE
                max_date_created > @last_update
            );

            REPLACE INTO flat_onc_htn_dm_triage_sync_queue
            (SELECT 
                person_id
              FROM 
		            amrs.person 
	            WHERE 
                date_voided > @last_update
            );

            REPLACE INTO flat_onc_htn_dm_triage_sync_queue
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
        SET @loop_start_time := now();

        -- Create temporary table with a set of person ids             
        DROP TEMPORARY TABLE IF EXISTS flat_onc_htn_dm_triage_build_queue__0;

        SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_onc_htn_dm_triage_build_queue__0 (person_id INT PRIMARY KEY) (SELECT * FROM ', @queue_table, ' LIMIT ', cycle_size, ');');
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        DROP TEMPORARY TABLE IF EXISTS flat_onc_htn_dm_triage_0a;
        SET @dyn_sql := CONCAT(
            'CREATE TEMPORARY TABLE flat_onc_htn_dm_triage_0a
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
                flat_onc_htn_dm_triage_build_queue__0 t0 USING (person_id)
                    LEFT JOIN
                etl.flat_orders t2 USING (encounter_id)
            WHERE
                t1.encounter_type IN ', @encounter_types, ');');
                            
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        INSERT INTO flat_onc_htn_dm_triage_0a
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
          flat_onc_htn_dm_triage_build_queue__0 t0 USING (person_id)
        );

        DROP TEMPORARY TABLE IF EXISTS flat_onc_htn_dm_triage_0;
        CREATE TEMPORARY TABLE flat_onc_htn_dm_triage_0 (INDEX encounter_id (encounter_id), INDEX person_enc (person_id, encounter_datetime))
        (SELECT 
            * 
        FROM
          flat_onc_htn_dm_triage_0a
        ORDER BY
          person_id, DATE(encounter_datetime)
        );
                        
		SET @cur_visit_type := null;
        SET @temperature := null;
        SET @waist_circumference := null;
        SET @weight := null;
        SET @height := null;
        SET @bmi := null;
        SET @systolic_bp := null;
        SET @diastolic_bp := null;
        SET @fasting_blood_glucose := null;
        SET @random_blood_glucose := null;

        DROP TEMPORARY TABLE IF EXISTS flat_onc_htn_dm_triage_1;

        CREATE TEMPORARY TABLE flat_onc_htn_dm_triage_1
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
                  WHEN obs REGEXP '!!1839=7037!!' THEN @cur_visit_type := 1
                  WHEN obs REGEXP '!!1839=7875!!' THEN @cur_visit_type := 2
                  ELSE @cur_visit_type := null
              END AS cur_visit_type,
              CASE
				          WHEN timestampdiff(YEAR, p.birthdate, curdate()) > 0 THEN round(TIMESTAMPDIFF(YEAR, p.birthdate, curdate()), 0)
				          ELSE ROUND(TIMESTAMPDIFF(MONTH, p.birthdate, curdate()) / 12, 2)
			        END AS age,
              p.gender,
              CASE
                  WHEN obs REGEXP '!!5088=' then @temperature := GetValues(obs, 5088)
              END AS temperature,
              CASE
                  WHEN obs REGEXP '!!7231=' then @waist_circumference := GetValues(obs, 7231)
              END AS waist_circumference,
              CASE
                  WHEN obs REGEXP '!!5089=' then @weight := GetValues(obs, 5089)
              END AS weight,
              CASE
                  WHEN obs REGEXP '!!5090=' then @height := GetValues(obs, 5090)
              END AS height,
              CASE
                  WHEN obs REGEXP '!!1342=' then @bmi := GetValues(obs, 1342)
              END AS bmi,
              CASE
                  WHEN obs REGEXP '!!5085=' then @systolic_bp := GetValues(obs, 5085)
              END AS systolic_bp,
              CASE
                  WHEN obs REGEXP '!!5086=' then @diastolic_bp := GetValues(obs, 5086)
              END AS diastolic_bp,
              CASE
                  WHEN obs REGEXP '!!6252=' then @fasting_blood_glucose := GetValues(obs, 6252)
              END AS fasting_blood_glucose,
              CASE
                  WHEN obs REGEXP '!!887=' then @random_blood_glucose := GetValues(obs, 887)
              END AS random_blood_glucose
          FROM 
              flat_onc_htn_dm_triage_0 t1
		      LEFT JOIN
		          amrs.location `l` ON l.location_id = t1.location_id
			  JOIN amrs.person p using (person_id)
   	      ORDER BY person_id, date(encounter_datetime) DESC
  	    );

          SET @prev_id = null;
          SET @cur_id = null;
						
          ALTER TABLE flat_onc_htn_dm_triage_1 DROP prev_id, DROP cur_id;

	        SELECT 
              COUNT(*)
          INTO 
              @new_encounter_rows
          FROM
              flat_onc_htn_dm_triage_1;
                              
          SELECT @new_encounter_rows;
          SET @total_rows_written := @total_rows_written + @new_encounter_rows;
          SELECT @total_rows_written;

          SET @dyn_sql := CONCAT('replace into ', @write_table, 										  
              '(SELECT
                    null,
                    person_id,
                    encounter_id,
                    encounter_type,
                    location_id,
					          location_name,
                    encounter_datetime,
                    cur_visit_type,
                    age,
                    gender,
                    temperature,
                    waist_circumference,
                    weight,
                    height,
                    bmi,
                    systolic_bp,
                    diastolic_bp,
                    fasting_blood_glucose,
                    random_blood_glucose
                FROM 
                    flat_onc_htn_dm_triage_1 t1
                        JOIN 
                    amrs.location t2 USING (location_id))'
          );

          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;


          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_onc_htn_dm_triage_build_queue__0 t2 using (person_id);'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                                
          SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                    
          SET @cycle_length = timestampdiff(second,@loop_start_time,now());
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
END