(in-package #:cl-db)

(defun get-row-value (column-name &rest row)
  (rest (assoc column-name row :test #'equal)))

(defmacro with-row-values (values row &body body)
  `(let (,@(mapcar #'(lambda (var-column-pair)
		       (destructuring-bind (binding column-name)
			   var-column-pair
			 `(,binding (apply #'get-row-value
					   ,column-name
					   ,row))))
		   values))
     ,@body))

(defparameter *tables-query*
  "SELECT table_schema, table_name
     FROM information_schema.tables
    WHERE table_schema <> 'pg_catalog'
      AND table_schema <> 'information_schema'
      AND table_type = 'BASE TABLE'")

(defparameter *columns-query*
  "SELECT columns.table_schema,
          columns.table_name,
          columns.column_name,
          columns.data_type
     FROM information_schema.columns as columns
    INNER JOIN information_schema.tables as tables
       ON columns.table_schema = tables.table_schema
      AND columns.table_name = tables.table_name
    WHERE tables.table_schema <> 'pg_catalog'
      AND tables.table_schema <> 'information_schema'
      AND tables.table_type = 'BASE TABLE'")

(defparameter *primary-key-columns-query*
  "SELECT columns.table_schema,
          columns.table_name,
          columns.column_name
     FROM information_schema.table_constraints as keys
    INNER JOIN information_schema.tables as tables
       ON keys.table_schema = tables.table_schema
      AND keys.table_name = tables.table_name
    INNER JOIN information_schema.constraint_column_usage as columns
       ON keys.constraint_name = columns.constraint_name
    WHERE tables.table_schema <> 'pg_catalog'
      AND tables.table_schema <> 'information_schema'
      AND tables.table_type = 'BASE TABLE'
      AND keys.constraint_type = 'PRIMARY KEY'")

(defparameter *foreign-keys-query*
  "SELECT keys.table_schema,
          keys.table_name,
          keys.constraint_name,
          referenced_tables.table_schema as referenced_table_schema,
          referenced_tables.table_name as referenced_table_name
     FROM information_schema.table_constraints as keys
    INNER JOIN information_schema.tables as tables
       ON keys.table_schema = tables.table_schema
      AND keys.table_name = tables.table_name
    INNER JOIN information_schema.constraint_table_usage as referenced_tables
       ON referenced_tables.constraint_name = keys.constraint_name
    WHERE tables.table_schema <> 'pg_catalog'
      AND tables.table_schema <> 'information_schema'
      AND tables.table_type = 'BASE TABLE'
      AND keys.constraint_type = 'FOREIGN KEY'")

(defparameter *foreign-keys-columns-query*
  "SELECT keys.constraint_name,
          columns.column_name
     FROM information_schema.table_constraints as keys
    INNER JOIN information_schema.tables as tables
       ON keys.table_schema = tables.table_schema
      AND keys.table_name = tables.table_name
    INNER JOIN information_schema.constraint_column_usage as columns
       ON keys.constraint_name = columns.constraint_name
    WHERE tables.table_schema <> 'pg_catalog'
      AND tables.table_schema <> 'information_schema'
      AND tables.table_type = 'BASE TABLE'
      AND keys.constraint_type = 'FOREIGN KEY'")

(defparameter *unique-constraint-query*
  "SELECT keys.table_schema,
          keys.table_name,
          keys.constraint_name
     FROM information_schema.table_constraints as keys
    INNER JOIN information_schema.tables as tables
       ON keys.table_schema = tables.table_schema
      AND keys.table_name = tables.table_name
    WHERE tables.table_schema <> 'pg_catalog'
      AND tables.table_schema <> 'information_schema'
      AND tables.table_type = 'BASE TABLE'
      AND keys.constraint_type = 'UNIQUE'")

(defparameter *unique-constraint-columns-query*
  "SELECT columns.table_schema,
          columns.table_name,
          keys.constraint_name,
          columns.column_name
     FROM information_schema.table_constraints as keys
    INNER JOIN information_schema.tables as tables
       ON keys.table_schema = tables.table_schema
      AND keys.table_name = tables.table_name
    INNER JOIN information_schema.constraint_column_usage as columns
       ON keys.constraint_name = columns.constraint_name
    WHERE tables.table_schema <> 'pg_catalog'
      AND tables.table_schema <> 'information_schema'
      AND tables.table_type = 'BASE TABLE'
      AND keys.constraint_type = 'UNIQUE'")

(defun load-columns (schema-name table-name &rest columns)
  (loop for column-row in columns
     when (and
	   (string= schema-name
		    (apply #'get-row-value "table_schema" column-row))
	   (string= table-name
		    (apply #'get-row-value "table_name" column-row)))
       collect (with-row-values ((column-name "column_name")
				 (sql-type "data_type")) column-row
		 (make-instance 'column
				:name column-name
				:sql-type sql-type))))

(defun load-primary-key-columns (schema-name table-name &rest columns)
  (loop for column-row in columns
     when (and
	   (string=
	    (apply #'get-row-value "table_schema" column-row)
	    schema-name)
	   (string=
	    (apply #'get-row-value "table_name" column-row)
	    table-name))
     collect (with-row-values ((column-name "column_name"))
		 column-row
	       column-name)))

(defun get-table (schema-name table-name &rest tables)
  (find-if #'(lambda (table)
	       (and (string= (schema-name-of table) table-schema)
		    (string= (name-of table) table-name)))
	   tables))

(defun load-foreign-key-columns (schema-name table-name
				 &rest foreign-keys-column-rows)
  (remove-if #'(lambda (foreign-keys-column-row)
		 (with-row-values ((foreign-key-name "constraint_name")
				   (column-name "column_name"))
		     foreign-keys-column-row
		   column-name))
	     foreign-keys-column-rows))

(defun load-foreign-key (foreign-keys-row
			 foreign-keys-columns-rows &rest tables)
  (with-row-values ((table-schema "table_schema")
		    (table-name "table_name")
		    (name "constraint_name")
		    (referenced-table-schema "referenced_table_schema")
		    (referenced-table-name "referenced_table_name"))
      foreign-key-row
    (push
     (make-instance 'foreign-key :name name
		    :columns (apply #'load-foreign-key-columns name
				    foreign-keys-columns-rows)
		    :table (apply #'get-table
				  table-schema
				  table-name
				  tables)
		    :referenced-table (apply #'get-table
					     referenced-table-schema
					     referenced-table-name
					     tables)))))

(defun get-tables (connection)
  (let ((column-rows
	 (cl-postgres:exec-query connection
				 *columns-query*
				 'cl-postgres:alist-row-reader))
	(table-rows
	 (cl-postgres:exec-query connection
				 *tables-query*
				 'cl-postgres:alist-row-reader))
	(primary-key-columns
	 (cl-postgres:exec-query connection
				 *primary-key-columns-query*
				 'cl-postgres:alist-row-reader)))
    (loop for table-row in table-rows collect
       	 (with-row-values ((table-name "table_name")
			   (schema-name "table_schema"))
	     table-row
	   (make-instance 'table :name table-name
			  :schema-name schema-name
			  :columns (apply #'load-columns
					  schema-name
					  table-name
					  columns)
			  :primary-key (apply #'load-primary-key
					      schema-name
					      table-name
					      primary-key-columns))))))




(defun get-database-schema (connection)
  (let ((tables (get-tables connection))
	(foreign-keys-rows
	 (cl-postgres:exec-query connection
				 *foreign-keys-query*
				 'cl-postgres:list-row-reader))
	(foreign-keys-columns
	 (cl-postgres:exec-query connection
				 *foreign-keys-columns-query*
				 'cl-postgres:list-row-reader)))
    (loop for table in tables
       do (setf (foreign-keys-of table)
		(mapcar #'(lambda (foreign-key-row)
			    (apply #'load-foreign-key table
				   foreign-key-row
				   foreign-keys-columns))
			(remove-if-not
			 #'(lambda (foreign-key-row)
			     (with-row-values ((table-schema "table_schema")
					       (table-name "table_name"))
				 foreign-key-row
			       (and
				(string= (name-of table) table-name)
				(string= (schema-name-of table) table-schema))))
			 foreign-keys-rows))))))

(defgeneric create-script (object))

(defmethod create-script ((instance mapping-schema))
  (mapcar #'create-script (tables-of instance)))

(defmethod create-script ((instance table))
  (format nil "CREATE TABLE ~a (~% ~{ ~a~^,~% ~}~%);"
	  (name-of instance)
	  (mapcar #'create-script (columns-of instance))))

(defmethod create-script ((instance column))
  (format nil "~a ~a"
	  (name-of instance)
	  (sql-type-of instance)))