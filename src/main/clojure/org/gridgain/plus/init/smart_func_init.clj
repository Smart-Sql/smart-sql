(ns org.gridgain.plus.init.smart-func-init
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.init.SmartFuncInit
        ; 是否生成 class 的 main 方法
        :main false
        ))


(def func-smart #{"println" "first" "rest" "next" "second" "last" "query_sql" "noSqlCreate" "noSqlGet" "noSqlInsertTran" "noSqlUpdateTran" "noSqlDeleteTran"
                  "noSqlDrop" "noSqlInsert" "noSqlUpdate" "noSqlDelete" "auto_id" "trans" "my_view" "rm_view" "add_scenes_to" "rm_scenes_from"
                  "add_job" "remove_job" "job_snapshot" "add_func" "remove_func" "recovery_to_cluster" "create_train_matrix" "has_train_matrix"
                  "drop_train_matrix" "train_matrix" "fit" "predict" "train_matrix_single" "loadCsv" "loadCode" "show_cache_name"})

(def func-set #{"add" "set" "take" "drop" "nth" "concat" "concat_ws" "contains?" "put" "get" "remove" "pop" "peek"
                "takeLast" "dropLast" "null?" "notNull?" "empty?" "notEmpty?" "nullOrEmpty?" "notNullOrEmpty?"
                "str_replace" "str_split" "str_find" "format" "regular" "range" "length" "ucase" "day_name" "to_number"
                "add_months" "to_date" "last_day" "trunc" "substr" "instrb" "chr" "months_between" "replace"
                "substrb" "lengthb" "tolowercase" "lowercase" "toUpperCase" "uppercase" "show_msg"
                "add_year" "add_quarter" "add_month" "add_date" "add_hour" "add_second" "add_ms"
                "diff_year" "diff_quarter" "diff_month" "diff_date" "diff_hour" "diff_second" "diff_ms"
                })

(def db-func-set #{"avg" "count" "max" "min" "sum" "abs" "acos" "asin" "atan" "cos" "cosh" "cot" "sin" "sinh" "tan" "tanh" "atan2"
                   "bitand" "bitget" "bitor" "bitxor" "mod" "pow" "sqrt"
                   "ceiling" "degrees" "exp"
                   "floor" "ln" "log" "log10" "pi" "radians" "round" "random_uuid" "roundMagic" "sign" "zero"
                   "ascii" "char" "concat" "concat_ws" "instr" "lower" "upper" "left" "right" "locate" "position" "soundex" "space" "lpad" "rpad"
                   "ltrim" "rtrim" "trim" "stringencode" "stringtoutf8" "to_char" "current_date" "current_time" "current_timestamp"
                   "dayname"
                   "day_of_month" "day_of_week" "day_of_year" "hour" "minute" "monthname" "quarter" "year" "month" "rand" "translate"
                   "decode" "greatest" "least" "ifnull" "nullif" "nvl2"})


(def db-func-no-set #{"SECURE_RAND" "ENCRYPT" "DECRYPT" "TRUNCATE" "COMPRESS" "EXPAND" "BIT_LENGTH"
                      "LENGTH" "OCTET_LENGTH" "DIFFERENCE" "HEXTORAW" "RAWTOHEX" "REGEXP_REPLACE" "REGEXP_LIKE" "DATEADD" "DATEDIFF"
                      "EXTRACT" "FORMATDATETIME" "PARSEDATETIME" "CASEWHEN" "CAST" "CONVERT" "TABLE"})



