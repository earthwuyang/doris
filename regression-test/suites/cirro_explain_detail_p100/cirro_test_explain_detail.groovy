
suite("cirro_test_explain_detail") {
    def fetchFileContent = { String url ->
        def command = "curl -s $url"
        def proc = command.execute()
        proc.waitFor()
        return proc.text
    }

    def getExplainDetailUrl = { String sqlQuery ->
        def res = sql sqlQuery
        return res[0][0].toString()
    }

    def executeAndSetSqlProperties = { boolean enableNereidsPlanner, boolean enablePipelineEngine ->
        sql "set experimental_enable_nereids_planner = ${enableNereidsPlanner}"
        sql "set enable_pipeline_x_engine = ${enablePipelineEngine}"
        sql "set enable_pipeline_engine = ${enablePipelineEngine}"

        return [
            enableNereidsPlanner ? '"is_nereids":1' : '"is_nereids":0',
            enablePipelineEngine ? '"Engine Type":"Pipeline"' : '"Engine Type":"Normal"'
        ]
    }

    // Initial SQL setup
    sql "show tables"
    def duplicate_table_name = "cirro_test_duplicate_table"
    sql """ DROP TABLE IF EXISTS ${duplicate_table_name} """
    sql """
        CREATE TABLE ${duplicate_table_name} (
          `k1` BIGINT NOT NULL,
          `k2` LARGEINT NOT NULL,
          `v1` BOOLEAN NULL DEFAULT 'true',
          `v2` TINYINT NULL,
          `v3` SMALLINT NULL,
          `v4` INT NULL,
          `v5` FLOAT NULL,
          `v6` DOUBLE NULL,
          `v7` DECIMAL(27, 9) NULL,
          `v8` DATE NULL,
          `v9` DATETIME(5) NULL,
          `v10` CHAR(10) NULL,
          `v11` VARCHAR(9) NULL,
          `v12` STRING NULL,
          `v13` JSON NULL,
          `v14` MAP<STRING,INT> NULL,
          `v15` ARRAY<INT> NULL,
          `v16` STRUCT<s_id:INT,s_name:STRING,s_address:STRING> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION p1 VALUES [("-9223372036854775808"), ("100000")),
        PARTITION p2 VALUES [("100000"), ("1000000000")),
        PARTITION p3 VALUES [("1000000000"), ("10000000000")),
        PARTITION p4 VALUES [("10000000000"), (MAXVALUE)))
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Define insertQuery
    def insertQuery = """
        EXPLAIN DETAIL INSERT INTO ${duplicate_table_name}
        (k1, k2, v1, v2, v9, v10, v11, v12) VALUES (
            3, 5555555555, default, 32, '2024-04-10 14:00:00.34567', 'Value30', 'Value31', 'Yet another string value'
        )
    """

    // Define sqlQueries array
    def sqlQueries = [
        ["EXPLAIN DETAIL SELECT * FROM  ${duplicate_table_name}", false, false],
        ["EXPLAIN DETAIL SELECT k1, k2, v3 FROM  ${duplicate_table_name}", true, false],
        ["EXPLAIN DETAIL SELECT k1, k2, v3, v8 FROM  ${duplicate_table_name}", true, true],
        ["EXPLAIN DETAIL SELECT * FROM  ${duplicate_table_name} WHERE k1 = 3", false, true],
        [insertQuery, false, false],
        [insertQuery, true, false],
        [insertQuery, true, true],
        [insertQuery, false, true]
    ]

    // Iterate over sqlQueries
    sqlQueries.each { query ->
        def sqlQuery = query[0]
        def enableNereidsPlanner = query[1]
        def enablePipelineEngine = query[2]
        def expectedContent = executeAndSetSqlProperties(enableNereidsPlanner, enablePipelineEngine)

        // Get EXPLAIN DETAIL URL and file content
        def url = getExplainDetailUrl(sqlQuery)
        def fileContent = fetchFileContent(url)

        // Assertion
        expectedContent.each { expected ->
            assertTrue(fileContent.contains(expected), "File should contain $expected")
        }
    }
}
