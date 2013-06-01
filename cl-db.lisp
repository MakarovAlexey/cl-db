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

(defmethod initialize-instance :after ((instance class-mapping-definition)
				       &key many-to-one-mappings
				       one-to-many-mappings)
  (dolist (mapping (append many-to-one-mappings one-to-many-mappings))
    (setf (class-mapping-definition-of mapping) instance)))

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
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defun map-slot (slot-name mapping &optional marshaller unmarshaller)
  (make-instance 'slot-mapping-definition
		 :marshaller marshaller
		 :slot-name slot-name
		 :mapping mapping
		 :unmarshaller unmarshaller))

(defclass value-mapping-definition ()
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun value (column &rest columns)
  (make-instance 'value-mapping-definition
		 :columns-names (list* column columns)))

(defclass one-to-many-mapping-definition ()
  ((class-mapping-definition :accessor class-mapping-definition-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun one-to-many (class-name column &rest columns)
  (make-instance 'one-to-many-mapping-definition
		 :mapped-class (find-class class-name)
		 :columns-names (list* column columns)))

(defclass many-to-one-mapping-definition ()
  ((class-mapping-definition :accessor class-mapping-definition-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (columns-names :initarg :columns-names
		  :reader columns-names-of)))

(defun many-to-one (class-name column &rest columns)
  (make-instance 'many-to-one-mapping-definition 
		 :mapped-class (find-class class-name)
		 :columns-names (list* column columns)))

(defclass mapping-schema ()
  ((class-mappings :initform (make-hash-table)
		   :reader class-mappings-of)
   (tables :initform (make-hash-table)
	   :reader tables-of)))

(defun list-mappings (mapping-schema)
  (alexandria:hash-table-values (class-mappings-of mapping-schema)))

(defun get-table (name mapping-schema)
  (gethash name (tables-of mapping-schema)))

(defclass class-mapping (undefined-class-mapping)
  ((table :initarg :table
	  :reader table-of)
   (value-mappings :reader value-mappings-of)
   (reference-mappings :reader reference-mappings-of)
   (subclasses-mappings :initform (list)
			:reader subclasses-mappings-of)
   (superclasses-mappings :initarg :superclasses-mappings
			  :reader superclasses-mappings-of)))

(defclass association ()
  ((foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass unidirectional-many-to-one-association (association)
  ((many-to-one-direction :initarg :many-to-one-direction
			  :reader many-to-one-direction-of)))

(defclass unidirectional-one-to-many-association (association)
  ((one-to-many-direction :initarg :one-to-many-direction
			  :reader one-to-many-direction-of)))

(defclass bidirectional-association
    (unidirectional-many-to-one-association
     unidirectional-one-to-many-association)
  ())

(defclass value-column (column) ())

(defclass slot-mapping ()
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (mapping :initarg :mapping :reader mapping-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping ()
  ((columns :initarg :columns :reader columns-of)))

(defclass reference-mapping ()
  ((referenced-class-mapping :initarg :referenced-class-mapping
			     :reader referenced-class-mapping-of)
   (association :initarg :association
		:reader association-of)))

(defclass one-to-many-mapping (reference-mapping) ())

(defclass many-to-one-mapping (reference-mapping) ())

(defclass table ()
  ((name :initarg :name :reader name-of)
   (columns :initform (make-hash-table) :reader columns-of)
   (primary-key :initarg :primary-key :reader primary-key-of)
   (foreign-keys :initform (make-hash-table) :reader foreign-keys-of)))

(defun get-column (name table)
  (gethash name (columns-of table))) 

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

(defun compute-columns (class-mapping-definition class-mapping-definitions)
  (let ((columns (make-hash-table)))
    (dolist (many-to-one-mapping (many-to-one-mappings-of class-mapping-definition))
      (dolist (column-name (columns-names-of many-to-one-mapping))
	(setf (gethash column-name columns)
	      (make-instance 'column :name column-name))))
    (let ((mapped-class (mapped-class-of class-mapping-definition)))
      (dolist (class-mapping-definition class-mapping-definitions columns)
	(dolist (one-to-many-mapping (remove-if-not #'(lambda (mapping)
							(eq mapped-class 
							    (mapped-class-of mapping)))
						    (one-to-many-mappings-of class-mapping-definition)))
	  (dolist (column-name (columns-names-of one-to-many-mapping))
	    (setf (gethash column-name columns)
		  (make-instance 'column :name column-name))))))
    (dolist (value-mapping (value-mappings-of class-mapping-definition))
      (dolist (column (columns-of value-mapping))
	(multiple-value-bind (column presentp)
	    (gethash (name-of column) columns)
	  (when presentp
	    (error "Column ~a already mapped" column))
	  (setf (gethash (name-of column) columns) column))))
    columns))

(defun compute-tables (class-mapping-definitions)
  (let ((tables (make-hash-table :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions tables)
      (let* ((table-name (table-name-of class-mapping-definition))
	     (columns (compute-columns class-mapping-definition
				       class-mapping-definitions)))
	(setf (gethash table-name tables)
	      (make-instance 'table :name table-name
			     :column columns
			     :primary-key (mapcar #'(lambda (name)
						      (gethash name columns))
						  (primary-key-of class-mapping-definition))))))))

(defun allocate-class-mappings (class-mapping-definitions)
  (let ((class-mappings (make-hash-table :size (length class-mapping-definitions))))
    (dolist (class-mapping-definition class-mapping-definitions class-mappings)
      (setf (gethash (mapped-class-of class-mapping-definition) class-mappings)
	    (allocate-instance 'class-mapping)))))

(defun reference-definition-key (reference-definition)
  (list* (mapped-class-of reference-definition)
	 (columns-names-of reference-definition)))

(defun compute-reference-pairs (class-mapping-definitions)
  (let ((associations (list)))
    (dolist (class-mapping-definition class-mapping-definitions associations)
      (let ((many-to-one-definitions (many-to-one-mappings-of class-mapping-definition))
	    (one-to-many-definitions (one-to-many-mappings-of class-mapping-definition)))
	(dolist (many-to-one-definition many-to-one-definitions)
	  (when (not (null (assoc many-to-one-definition associations)))
	    (setf associations
		 (acons many-to-one-definition
			(find (reference-definition-key many-to-one-definition)
			      one-to-many-definitions
			      :test #'equal
			      :key #'reference-definition-key)
			associations))))
	(dolist (one-to-many-definition one-to-many-definitions)
	  (when (not (null (rassoc one-to-many-definition associations)))
	    (setf associations
		 (acons (find (reference-definition-key one-to-many-definition)
			      many-to-one-definitions
			      :test #'equal
			      :key #'reference-definition-key)
			one-to-many-definition
			associations))))))))

(defgeneric compute-foreign-key (reference-definition mapping-schema))

(defmethod compute-foreign-key ((many-to-one-definition many-to-one-mapping-definition)
				mapping-schema)
  (let ((table (table-of (get-mapping
			  (mapped-class-of 
			   (class-mapping-definition-of many-to-one-definition))
			  mapping-schema))))
  (make-instance 'foreign-key
		 :table table
		 :columns (mapcar #'(lambda (name)
				      (get-column name table))
				  (columns-names-of many-to-one-definition))
		 :referenced-table (get-mapping
				    (mapped-class-of many-to-one-definition)
				    mapping-schema))))

(defmethod compute-foreign-key ((one-to-many-definition one-to-many-mapping-definition)
				mapping-schema)
  (let ((table (table-of (get-mapping
			  (mapped-class-of one-to-many-definition)
			  mapping-schema))))
  (make-instance 'foreign-key
		 :table table
		 :columns (mapcar #'(lambda (name)
				      (get-column name table))
				  (columns-names-of one-to-many-definition))
		 :referenced-table (table-of
				    (get-mapping
				     (mapped-class-of 
				      (class-mapping-definition-of one-to-many-definition))
				      mapping-schema)))))

(defgeneric compute-association (many-to-one-definition
				 one-to-many-definition
				 mapping-schema))

(defmethod compute-association ((many-to-one-definition many-to-one-mapping-definition)
				(one-to-many-definition (eql nil))
				mapping-schema)
  (make-instance 'unidirectional-many-to-one-association
		 :foreign-key (compute-foreign-key many-to-one-definition mapping-schema)
		 :many-to-one-definition many-to-one-definition
		 :mapping-schema mapping-schema))

(defmethod compute-association ((many-to-one-definition (eql nil))
				(one-to-many-definition one-to-many-mapping-definition)
				mapping-schema)
  (make-instance 'unidirectional-one-to-many-association
		 :foreign-key (compute-foreign-key one-to-many-definition mapping-schema)
		 :one-to-many-definition one-to-many-definition
		 :mapping-schema mapping-schema))

(defmethod compute-association ((many-to-one-definition many-to-one-mapping-definition)
				(one-to-many-definition one-to-many-mapping-definition)
				mapping-schema)
  (make-instance 'bidirectional-association
		 :foreign-key (compute-foreign-key many-to-one-definition mapping-schema)
		 :many-to-one-definition many-to-one-definition
		 :one-to-many-definition one-to-many-definition
		 :mapping-schema mapping-schema))

(defmethod initialize-instance :after ((instance unidirectional-many-to-one-association)
				       &key many-to-one-definition
				       mapping-schema)
  (with-slots (many-to-one-direction) instance
    (setf many-to-one-direction
	  (make-instance 'many-to-one-direction
			 :association instance
			 :slot-name (slot-name-of many-to-one-definition)
			 :unmarshaller (unmarshaller-of many-to-one-definition)
			 :marshaller (marshaller-of many-to-one-definition)
			 :referenced-class-mapping (get-mapping
						    (mapped-class-of many-to-one-definition)
						    mapping-schema)))))

(defmethod initialize-instance :after ((instance unidirectional-one-to-many-association)
				       &key one-to-many-definition
				       mapping-schema)
  (with-slots (one-to-many-direction) instance
    (setf one-to-many-direction
	  (make-instance 'one-to-many-direction
			 :association instance
			 :slot-name (slot-name-of one-to-many-definition)
			 :unmarshaller (unmarshaller-of one-to-many-definition)
			 :marshaller (marshaller-of one-to-many-definition)
			 :referenced-class-mapping (get-mapping
						    (mapped-class-of one-to-many-definition)
						    mapping-schema)))))

(defun compute-associations (class-mapping-definitions mapping-schema)
  (mapcar #'(lambda (reference-pair)
	      (destructuring-bind
		    (many-to-one-definition . one-to-many-definition)
		  reference-pair
		(compute-association many-to-one-definition
				     one-to-many-definition
				     mapping-schema)))
	  (compute-reference-pairs class-mapping-definitions)))

(defun compute-association-directions-table (class-mapping-definitions mapping-schema)
  (let ((association-directions-table (make-hash-table)))
    (loop for (many-to-one-definition . one-to-many-definition)
	 in (compute-reference-pairs class-mapping-definitions)
	 for association = (compute-association many-to-one-definition
						one-to-many-definition
						mapping-schema)
       when (not (null many-to-one-definition))
       do (setf (gethash many-to-one-definition
			 association-directions-table)
		(many-to-one-direction-of association))
       when (not (null one-to-many-definition))
       do (setf (gethash many-to-one-definition
			 association-directions-table)
		(one-to-many-direction-of association))
       return association-directions-table)))

(defun compute-superclasses-mappings (class-mapping-definition mapping-schema)
  (mapcar #'(lambda (class)
	      (get-mapping class mapping-schema))
	  (mapped-superclasses-of class-mapping-definition)))

(defun compute-subclasses-mappings (class-mapping-definition mapping-schema)
  (let ((mapped-class (mapped-class-of class-mapping-definition)))
    (remove-if-not #'(lambda (class-mapping)
		       (find mapped-class
			     (mapped-superclasses-of class-mapping)))
		   (list-mappings mapping-schema))))

(defun compute-association-foreign-keys (table association-directions-table)
  (loop for direction being the hash-values in association-directions-table
     for foreign-key = (foreign-key-of (association-of direction))
     when (eq (table-of foreign-key) table)
     collect foreign-key))

(defun compute-superclasses-foreign-keys (table superclasses-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (make-instance 'foreign-key
			     :table table
			     :columns (primary-key-of table)
			     :referenced-table (table-of superclass-mapping)))
	  superclasses-mappings))

(defun compute-value-mappings (class-mapping-definition table)
  (mapcar #'(lambda (definition)
	      (make-instance 'value-mapping
			     :columns (mapcar #'(lambda (name)
						  (get-column name table))
					      (columns-names-of definition))
			     :slot-name (slot-name-of definition)
			     :marshaller (marshaller-of definition)
			     :unmarshaller (unmarshaller-of definition)))
	  (value-mappings-of class-mapping-definition)))

(defmethod initialize-instance :after ((instance mapping-schema)
				       &key class-mapping-definitions)
  (with-slots (tables class-mappings) instance
    (setf tables (compute-tables class-mapping-definitions)
	  class-mappings (allocate-class-mappings class-mapping-definitions)))
  (let ((association-directions-table
	 (compute-association-directions-table class-mapping-definitions
					       instance)))
    (dolist (definition class-mapping-definitions instance)
      (let ((mapped-class (mapped-class-of definition))
	    (table (get-table (table-name-of definition) instance))
	    (superclasses-mappings (compute-superclasses-mappings definition instance)))
	(setf (slot-value table 'foreign-keys)
	      (append (compute-superclasses-foreign-keys table superclasses-mappings)
		      (compute-association-foreign-keys table
							association-directions-table)))
	(initialize-instance
	 (get-mapping mapped-class instance)
	 :superclasses-mappings superclasses-mappings
	 :subclasses-mappings (compute-subclasses-mappings definition instance)
	 :value-mappings (compute-value-mappings definition table)
	 :mapped-class mapped-class
	 :table table
	 :reference-mappings (append
			      (mapcar #'(lambda (many-to-one-definition)
					  (gethash many-to-one-definition
						   association-directions-table))
				      (many-to-one-mappings-of definition))
			      (mapcar #'(lambda (one-to-many-definition)
					  (gethash one-to-many-definition
						   association-directions-table))
				      (one-to-many-mappings-of definition))))))))

(defun get-mapping (mapped-class mapping-schema)
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class (class-mappings-of mapping-schema))
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defclass clos-session ()
  ((mappings :initarg :mappings :reader mappings-of)
   (connection :initarg :connection :reader connection-of)
   (loaded-objects :initarg :loaded-objects :reader loaded-objects-of)))

(defun make-clos-session (connector mapping-schema)
  (make-instance 'clos-session
		 :connection (funcall connector)
		 :mapping-schema mapping-schema))

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