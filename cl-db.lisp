;;; cl-db.lisp

(in-package #:cl-db)

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (mapped-superclasses :initarg :mapped-superclasses
			:reader mapped-superclasses-of)
   (slot-mapping-definitions :initarg :slot-mapping-definitions
			     :reader slot-mapping-definitions-of)
   (value-mappings :initarg :value-mapping
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)))

(defun value-mapping-p (mapping)
  (typep mapping (find-class 'value-mapping-definition)))

(defun many-to-one-mapping-p (mapping)
  (typep mapping (find-class 'many-to-one-mapping-definition)))

(defun one-to-many-mapping-p (mapping)
  (typep mapping (find-class 'one-to-many-mapping-definition)))

(defun map-class (class-name table-name primary-key &key superclasses slots)
  (let ((mappings (mapcar #'mapping-of slots)))
    (make-instance 'class-mapping-definition
		   :table-name table-name
		   :primary-key primary-key
		   :mapped-class (find-class class-name)
		   :mapped-superclasses (mapcar #'find-class superclasses)
		   :slot-mapping-definitions slots
		   :value-mapping (remove-if-not #'value-mapping-p
						 mappings)
		   :many-to-one-mappings (remove-if-not #'many-to-one-mapping-p
							mappings)
		   :one-to-many-mappings (remove-if-not #'one-to-many-mapping-p
							mappings))))

(defclass slot-mapping-definition ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun map-slot (slot-name mapping &optional constructor reader)
  (make-instance 'slot-mapping-definition
		 :constructor constructor
		 :slot-name slot-name
		 :mapping mapping
		 :reader reader))

(defclass value-mapping-definition ()
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun value (column &rest columns)
  (make-instance 'value-mapping-definition
		 :columns-names (list* column columns)))

(defclass one-to-many-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun one-to-many (class-name column &rest columns)
  (make-instance 'one-to-many-mapping-definition
		 :mapped-class (find-class class-name)
		 :columns-names (list* column columns)))

(defclass many-to-one-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun many-to-one (class-name column &rest columns)
  (make-instance 'many-to-one-mapping-definition 
		 :mapped-class (find-class class-name)
		 :columns-names (list* column columns)))

(defclass class-mapping (undefined-class-mapping)
  ((table :initarg :table
	  :reader table-of)
   (value-mappings :reader value-mappings-of)
   (reference-mappings :reader reference-mappings-of)
   (subclasses-mappings :initform (list)
			:reader subclasses-mappings-of)
   (superclasses-mappings :initarg :superclasses-mappings
			  :reader superclasses-mappings-of)))

(defclass mapping-schema ()
  ((class-mappings :initfrom (make-hash-table)
		   :reader class-mappings-of)
   (tables :initfrom (make-hash-table)
	   :reader tables-of)))

(defun list-mappings (mapping-schema)
  (alexandria:hash-table-values (class-mappings-of mapping-schema)))

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initform (make-hash-table) :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initform (make-hash-table) :reader foreign-keys-of)))

(defclass column ()
  ((name :initarg :name :reader name-of)
   (type-name :initarg :type-name :reader type-name-of)))

(defclass foreign-key-column (column)
  ((foreign-keys :initarg :foreign-keys :reader foreign-keys-of)))

(defclass foreign-key ()
  ((table :initarg :table
	  :reader table-of)
   (columns :initarg :columns
	    :reader columns-of)
   (referenced-table :initarg :referenced-table
		     :reader referenced-table-of)))

;; implement column-type
(defun compute-columns (class-mapping-definition class-mapping-definitions)
  (let ((columns (make-hash-table)))
    (dolist (value-mapping (value-mappings-of class-mapping-definition))
      (dolist (column (columns-of value-mapping))
	(setf (gethash (name-of column) columns) column)))
    (dolist (many-to-one-mapping (many-to-one-mapping-of class-mapping-definition))
      (dolist (column-name (column-names-of many-to-one-mapping))
	(setf (gethash column-name columns)
	      (make-instance 'column :name column-name))))
    (let ((mapped-class (mapped-class-of class-mapping-definition)))
      (dolist (class-mapping-definition class-mapping-definitions columns)
	(dolist (one-to-many-mapping (remove-if-not #'(lambda (mapping)
							(eq mapped-class 
							    (mapped-class-of mapping)))
						    (one-to-many-mapping-of class-mapping-definition)))
	  (dolist (column-name (column-names-of one-to-many-mapping))
	    (setf (gethash column-name columns)
		  (make-instance 'column :name column-name))))))))

(defun compute-tables (class-mapping-definitions)
  (let ((tables (make-hash-table :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions tables)
      (setf (gethash (table-name-of class-mapping-definition) tables)
	    (make-instance 'table :columns (compute-columns class-mapping-definition
							    class-mapping-definitions))))))

(defun allocate-class-mappings (class-mapping-definitions)
  (let ((class-mappings (make-hash-table :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions class-mappings)
      (setf (gethash (mapped-class-of class-mapping-definition) class-mappings)
	    (allocate-instance 'class-mapping)))))

(defun ensure-association (table referenced-table columns associations)
  (multiple-value-bind (association presentp)
      (gethash (list* table referenced-table columns) associations)
    (declare (ignore association))
    (if (not presentp)
	(make-instance 'association
		       :foreign-key (make-instance 'foreign-key
						   :table table
						   :referenced-table referenced-table
						   :columns columns))
	association)))

(defclass associations ()
  ((associations-by-one-to-many :initform (make-hash-table))
   (associations-by-many-to-one :initform (make-hash-table))))

(defun compute-associations (class-mapping-definitions mapping-schema)
  (let ((associations (make-hash-table :test #'equal)))
    (dolist (class-mapping-definition class-mapping-definitions)
      (let ((table (get-table (table-name-of class-mapping-definition) mapping-schema))
	    (mapped-class (mapped-class-of class-mapping-definition)))
	(dolist (many-to-one-definition (many-to-one-mappings-of class-mapping-definition))
	  (multiple-value-bind (association presentp)
	      (gethash
	       (list* table
		      (get-table (table-name-of 
				  (find (mapped-class-of many-to-one-mapping)
					class-mapping-definitions
					:key #'mapped-class-of))
				 mapping-schema)
		      (mapcar #'(lambda (column-name)
				  (get-column column-name table))
			      (column-names-of many-to-one-mapping)))
	       associations)
	    (when (not presentp)
	      (make-instance 'association
			     :foreign-key (make-instance 'foreign-key
							 :table table
							 :referenced-table referenced-table
							 :columns columns)
			     :many-to-one (make-instance 'many-to-one-mapping
							 :mapped-class mapped-class
							 :referenced-class 
	(dolist (one-to-many-mapping (one-to-many-mappings-of class-mapping-definition))
	  (ensure-association (get-table (table-name-of
					  (find (mapped-class-of one-to-many-mapping)
						class-mapping-definitions
						:key #'mapped-class-of))
					 mapping-schema)
			      table
			      (mapcar #'(lambda (column-name)
					  (get-column column-name table))
				      (column-names-of one-to-many-mapping))
			      associations))))))

(defun compute-superclasses-mappings (class-mapping-definition mapping-schema)
  (mapcar #'(lambda (class)
	      (get-mapping class mapping-schema))
	  (superclasses-of class-mapping-definition)))

(defun compute-subclasses-mappings (class-mapping-definition mapping-schema)
  (let ((mapped-class (mapped-class-of class-mapping-definition)))
    (remove-if-not #'(lambda (class-mapping)
		       (find mapped-class
			     (superclasses-of class-mapping)))
		   (list-mappings mapping-schema))))

(defun compute-many-to-one-mappings (class-mapping-definition mapping-schema)
  (mapcar #'(lambda (slot-mapping-definition)
	      (let ((mapping-definition (mapping-of slot-mapping-definition)))
		(make-instance 'many-to-one
			       :slot-name (slot-name-of slot-mapping-definition)
			       :constructor (constructor-of slot-mapping-definition)
			       :reader (reader-of slot-mapping-definition)
			       :referenced-class-mapping (get-mapping
							  (mapped-class-of mapping-definition)
							  mapping-schema))))
	  (many-to-one-mappings-of class-mapping-definition)))

(defun compute-one-to-many-mappings (class-mapping-definition mapping-schema)
  (mapcar #'(lambda (slot-mapping-definition)
	      (let ((mapping-definition (mapping-of slot-mapping-definition)))
		(make-instance 'one-to-many
			       :slot-name (slot-name-of slot-mapping-definition)
			       :constructor (constructor-of slot-mapping-definition)
			       :reader (reader-of slot-mapping-definition)
			       :foreign
			       :referenced-mapping (get-mapping
						    (mapped-class-of mapping-definition)
						    mapping-schema))))
	  (one-to-many-mappings-of class-mapping-definition)))

(defmethod initialize-instance :after ((instance mapping-schema)
				       &key class-mapping-definitions)
  (with-slots (tables class-mappings) instance
    (setf tables (compute-tables class-mapping-definitions)
	  class-mappings (allocate-class-mappings class-mapping-definitions)))
  (let ((associations (compute-associations class-mapping-definitions instance)))
    (dolist (definition class-mapping-definitions instance)
      (let ((table (get-table (table-name-of definition) instance))
	    (mapped-class (mapped-class-of definition)))
	(initialize-instance
	 (get-mapping mapped-class instance)
	 :superclasses-mappings (compute-superclasses-mappings definition instance)
	 :subclasses-mappings (compute-subclasses-mappings definition instance)
	 :value-mappings (compute-value-mappings definition table)
	 :mapped-class mapped-class
	 :table table
	 :reference-mappings (append
			      (compute-one-to-many-mappings definition instance)
			      (compute-many-to-one-mappings definition table instance)))))
    
(defclass value-column (column) ())

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (constructor :initarg :constructor :reader constructor-of)
   (reader :initarg :reader :reader reader-of)))

(defun get-mapping-definition (mapped-class class-mapping-definitions)
  (gethash mapped-class class-mapping-definitions))

(defun get-mapping (mapped-class class-mappings)
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class class-mappings)
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defun compute-slot-mappings (class-mapping slot-mapping-definitions
			      class-mappings)
  (mapcar #'(lambda (definition)
	      (make-instance 'slot-mapping
			     :slot-name (slot-name-of definition)
			     :mapping (compute-slot-mapping (mapping-of definition)
							    class-mapping
							    class-mappings)))
	  slot-mapping-definitions))

(defun compute-mappings (class-mapping-definitions)
  (let ((class-mappings (reduce #'(lambda (hash-table definition)
				    (let ((mapped-class (mapped-class-of definition)))
				      (setf (gethash mapped-class hash-table)
					    (make-instance 'class-mapping
							   :mapped-class mapped-class
							   :table (make-instance 'table
										 :name (table-name-of definition)
										 :primary-key (primary-key-of definition)))))
				    hash-table)
				class-mapping-definitions
				:initial-value (make-hash-table :size (length class-mapping-definitions)))))
    (dolist (definition class-mapping-definitions class-mappings)
      (let ((class-mapping (get-mapping (mapped-class-of definition)
					class-mappings)))
	(with-slots (superclasses-mappings slot-mappings) class-mapping
	  (setf superclasses-mappings
		(mapcar #'(lambda (superclass)
			    (let ((superclass-mapping (get-mapping (mapped-class-of superclass)
								   class-mappings)))
			      (push class-mapping (subclasses-mappings-of superclass-mapping))
			      superclass-mapping))
			(mapped-superclasses-of definition))
		slot-mappings
		(compute-slot-mappings class-mapping
				       (slot-mapping-definitions-of definition)
				       class-mappings)))))))

(defgeneric compute-slot-mapping (mapping-definition
				  class-mapping
				  class-mapping-definitions))

(defclass value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defun column-exist-p (table column-name)
  (gethash column-name (columns-of table)))

(defun add-column (table column)
  (setf (gethash (name-of column) (columns-of table)) column))

(defmethod make-value-column (table name)
  (if (not (column-exist-p table name))
      (add-column table (make-instance 'value-column :name name))
      (error "Column ~a already mapped" name)))

(defmethod compute-slot-mapping ((mapping-definition value-mapping-definition)
				 class-mapping class-mappings)
  (declare (ignore class-mappings))
  (let ((table (table-of class-mapping)))
    (make-instance 'value-mapping
		   :columns (mapcar #'(lambda (column-name)
					(make-value-column table
							   column-name))
				    (columns-names-of mapping-definition)))))

(defclass reference-mapping ()
  ((referenced-class-mapping :initarg :referenced-class-mapping
			     :reader referenced-class-mapping-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass one-to-many-mapping (reference-mapping) ())

(defun ensure-foreign-key-column (table column-name)
  (gethash column-name (columns-of table)
	   (setf (gethash column-name (columns-of table))
		 (make-instance 'foreign-key-column
				:table table :name column-name))))

(defun ensure-foreign-key (table columns-names referenced-table)
  (let* ((columns (mapcar #'(lambda (column-name)
			      (ensure-foreign-key-column table column-name))
			  columns-names))
	 (key (cons table columns))
	 (foreign-keys (foreign-keys-of table)))
    (gethash key foreign-keys
	     (setf (gethash key foreign-keys)
		   (make-instance 'foreign-key
				  :table table :columns columns
				  :referenced-table referenced-table)))))

(defmethod compute-slot-mapping ((mapping-definition one-to-many-mapping-definition)
				 class-mapping class-mappings)
  (let ((referenced-class-mapping
	 (get-mapping (mapped-class-of mapping-definition)
		      class-mappings)))
    (make-instance 'one-to-many-mapping
		   :referenced-class-mapping referenced-class-mapping
		   :foreign-key (ensure-foreign-key (table-of referenced-class-mapping)
						    (columns-names-of mapping-definition)
						    (table-of class-mapping)))))

(defclass many-to-one-mapping (reference-mapping) ())

(defmethod compute-slot-mapping ((mapping-definition many-to-one-mapping-definition)
				 class-mapping class-mapping-definitions)
  (let ((referenced-class-mapping (get-mapping (mapped-class-of mapping-definition)
					       class-mapping-definitions)))
    (make-instance 'many-to-one-mapping
		   :referenced-class-mapping referenced-class-mapping
		   :foreign-key (ensure-foreign-key (table-of class-mapping)
						    (columns-names-of mapping-definition)
						    (table-of referenced-class-mapping)))))

;; наследование
;; в т.ч. проверка одинаковости первичного ключа и родительских классов

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (connection :initarg :connection :reader connection-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector &rest class-mappings)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :mappings (compute-mappings class-mappings)))

(defun get-class-mapping (session class)
  (multiple-value-bind (mapping present-p)
      (gethash class (mappings-of session))
    (if (not present-p)
	(error "Class ~a not mapped" class)
	mapping)))

(defclass table-reference ()
  ((table :initarg :table
	  :reader table-of)
   (alias :initarg :alias
	  :reader alias-of)))

(defclass joined-superclass ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (table-reference :reader table-reference-of)
   (joined-superclasses :initarg :joined-superclasses
			:reader joined-superclasses-of)))

(defclass object-loader (joined-superclass)
  ((subclass-object-loaders :initarg :subclass-object-loaders
			    :reader subclass-object-loaders-of)))

(defun compute-joined-superclasses (class-mapping)
  (let ((superclasses-mappings (superclasses-mappings-of class-mapping)))
    (mapcar #'(lambda (superclass-mapping)
		(make-instance 'joined-superclass
			       :class-mapping superclass-mapping))
	    superclasses-mappings)))

(defun compute-subclass-object-loaders (object-loader)
  (let ((class-mapping (class-mapping-of object-loader)))
    (mapcar #'(lambda (subclass-mapping)
		(make-instance 'object-loader
			       :class-mapping subclass-mapping
			       :joined-superclasses (compute-joined-superclasses
						     (remove class-mapping
							     (superclasses-mappings-of class-mapping)))))
	    (subclasses-mappings-of class-mapping))))

(defmethod initialize-instance :after ((instance object-loader)
				       &key class-mapping
				       (joined-superclasses
					(compute-joined-superclasses class-mapping)))
  (setf (slot-value instance 'joined-superclasses) joined-superclasses
	(slot-value instance 'subclass-object-loaders)
	(compute-subclass-object-loaders instance)))

(defvar *session*)

(defun call-with-session (session function)
  (let ((*session* session))
    (funcall function)))

(defmacro with-session ((session) &body body)
  `(call-with-session ,session #'(lambda () ,@body)))