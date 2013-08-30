;;; cl-db.lisp

(in-package #:cl-db)

(defclass column-definition ()
  ((column-name :initarg :column-name
		:reader column-name-of)
   (column-type :initarg :column-type
		:reader column-type-of)))

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)
   (superclasses :initarg :superclasses
		 :reader superclasses-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (many-to-one-mappings :initarg :many-to-one-mappings
			 :reader many-to-one-mappings-of)
   (one-to-many-mappings :initarg :one-to-many-mappings
			 :reader one-to-many-mappings-of)))

(defclass slot-mapping-definition ()
  ((slot-name :initarg :slot-name
	      :reader slot-name-of)
   (unmarshaller :initarg :unmarshaller
		 :reader unmarshaller-of)
   (marshaller :initarg :marshaller
	       :reader marshaller-of)))

(defclass value-mapping-definition
    (slot-mapping-definition)
  ((columns :initarg :columns
	    :reader columns-of)))

(defclass reference-mapping-definition
    (slot-mapping-definition)
  ((columns-names :initarg :columns-names
		  :reader columns-names-of)
   (referenced-class :initarg :referenced-class
		     :reader referenced-class-of)))

(defclass many-to-one-mapping-definition
    (reference-mapping-definition)
  ())

(defclass one-to-many-mapping-definition
    (reference-mapping-definition)
  ())

(defclass table ()
  ((name :initarg :name
	 :reader name-of)
   (columns :initarg :columns
	    :reader columns-of)
   (primary-key :initarg :primary-key
		:reader primary-key-of)))

(defclass column ()
  ((name :initarg :name
	 :reader name-of)
   (sql-type :initarg :sql-type
	     :reader sql-type-of)))

(defclass class-mapping ()
  ((table :reader table-of)
   (mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (value-mappings :reader value-mappings-of)
   (reference-mappings :reader reference-mappings-of)
   (subclasses-inheritance-mappings :reader subclasses-inheritance-mappings-of)
   (superclasses-inhertance :reader superclasses-inheritance-mappings-of)))

(defclass inheritance-mapping ()
  ((columns :initarg :columns
	    :reader columns-of)
   (subclass-mapping :initarg :subclass-mapping
		     :reader subclass-mapping-of)
   (superclass-mapping :initarg :superclass-mapping
		       :reader superclass-mapping-of)))

(defclass association ()
  ((columns :initarg :columns
	    :reader columns-of)
   (many-to-one-mapping :initarg :many-to-one-mapping
			:reader many-to-one-mapping-of)
   (one-to-many-mapping :initarg :one-to-many-mapping
			:reader one-to-many-mapping-of)))

(defclass slot-mapping ()
  ((path :initarg :path :reader path-of)
   (slot-name :initarg :slot-name :reader slot-name-of)
   (marshaller :initarg :marshaller :reader marshaller-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns :reader columns-of)))

(defclass reference-mapping (slot-mapping)
  ((association :initarg :association
		:reader association-of)
   (class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defclass many-to-one-mapping (reference-mapping) ())

(defclass one-to-many-mapping (reference-mapping) ())

(defclass mapping-schema ()
  ((tables :initarg :tables
	   :reader tables-of)
   (associations :initarg :associations
		 :reader associations-of)
   (class-mappings :initarg :class-mappings
		   :reader class-mappings-of)
   (inheritance-mappings :initarg :inheritance-mappings
			 :reader inheritance-mappings-of)))

(defun get-slot-name (class reader)
  (let ((reader-name (generic-function-name reader)))
    (loop for slot in (class-slots class)
       when (find reader-name (slot-definition-readers slot))
       return (slot-definition-name slot)
       finally (error "Slot for reader ~a not found" reader-name))))

(defun get-reference-mapping (class-mapping reference-reader)
  (multiple-value-bind (reference-mapping presentp)
      (gethash
       (get-slot-name (mapped-class-of class-mapping)
		      reference-reader)
       (reference-mappings-of class-mapping))
    (if (not presentp)
	(error "Mapped reference for accessor ~a not found"
	       reference-reader)
	reference-mapping)))

(defvar *class-mapping-definitions*)

(defun find-class-mapping-definition (mapped-class &optional
				      (class-mapping-definitions
				       *class-mapping-definitions*))
  (find mapped-class class-mapping-definitions :key #'mapped-class-of))

(defvar *mapping-schema*)

(defun get-class-mapping (mapped-class &optional
			  (mapping-schema *mapping-schema*))
  (multiple-value-bind (mapping present-p)
      (gethash mapped-class (class-mappings-of mapping-schema))
    (if (not present-p)
	(error "Mapping for class ~a not found" mapped-class)
	mapping)))

(defun get-table (name &optional (mapping-schema *mapping-schema*))
  (multiple-value-bind (mapping present-p)
      (gethash name (tables-of mapping-schema))
    (if (not present-p)
	(error "Table ~a not found" name)
	mapping)))

(defun get-column (name table)
  (multiple-value-bind (column present-p)
      (gethash name table)
    (if (not present-p)
	(error "Column ~a for table ~a not found" name (name-of table))
	column)))

(defun list-inhritance-mappings (&optional
				 (mapping-schema *mapping-schema*))
  (inheritance-mappings-of mapping-schema))

(defun value-mapping-p (slot-mapping-definition)
  (typep slot-mapping-definition 'value-mapping-definition))

(defun reference-mapping-p (slot-mapping-definition)
  (or (many-to-one-mapping-of slot-mapping-definition)
      (one-to-many-mapping-of slot-mapping-definition)))

(defun many-to-one-mapping-p (slot-mapping-definition)
  (typep slot-mapping-definition 'many-to-one-mapping-definition))

(defun one-to-many-mapping-p (slot-mapping-definition)
  (typep slot-mapping-definition 'one-to-many-mapping-definition))

(defgeneric find-column-type (column-name slot-mapping))

(defmethod find-column-type (column-name
			     (slot-mapping value-mapping-definition))
  (let ((column (assoc column-name (columns-of slot-mapping)
		       :test #'equal)))
    (when (not (null column))
      (sql-type-of column))))

(defmethod find-column-type (column-name
			     (slot-mapping many-to-one-mapping-definition))
  (let ((column-position (position column-name (columns-of slot-mapping))))
    (when (not (null column-position))
      (get-reference-column-type column-position
				 (mapped-class-of slot-mapping)))))

(defmethod find-column-type (column-name
			     (slot-mapping one-to-many-mapping-definition))
  (declare (ignore column-name slot-mapping))
  nil)

(defun get-column-type (column-name class-mapping-definition)
  (let ((column-type
	 (loop for slot-mapping-definition
	    in (value-mappings-of class-mapping-definition)
	    thereis (and (find-column-type column-name
					   slot-mapping-definition)))))
    (if (null column-type)
	(loop for slot-mapping-definition
	   in (many-to-one-mappings-of class-mapping-definition)
	   thereis (and (find-column-type column-name
					  slot-mapping-definition)))
	column-type)))

(defun get-reference-column-type (column-position mapped-class)
  (let* ((class-mapping-definition (find-class-mapping-definition mapped-class))
	 (primary-key (primary-key-of class-mapping-definition))
	 (superclasses (superclasses-of class-mapping-definition)))
    (if (not (null (or primary-key superclasses)))
	(if (not (null primary-key))
	    (get-column-type (elt primary-key column-position)
			     class-mapping-definition)
	    (get-reference-column-type column-position
				       (find-class-mapping-definition
					(first superclasses))))
	(error "For mapping of class ~a not specified primary key or superclasses"
	       mapped-class))))

(defun get-value-mapping-columns (class-mapping-definition)
  (loop for slot-mapping-definition
     in (value-mappings-of class-mapping-definition)
     append (columns-of slot-mapping-definition)))

(defun get-many-to-one-columns (class-mapping-definition)
  (loop for slot-mapping-definition
     in (many-to-one-mappings-of class-mapping-definition)
     for columns = (columns-of slot-mapping-definition)
     append (mapcar #'(lambda (column-name)
			(make-instance 'column :name column-name
				       :sql-type
				       (get-reference-column-type
					(position column-name columns)
					(mapped-class-of slot-mapping-definition))))
		    columns)))
				 
(defun get-one-to-many-columns (class-mapping-definition &optional
				(class-mapping-definitions
				 *class-mapping-definitions*))
  (let ((mapped-class (mapped-class-of class-mapping-definition)))
    (loop for class-mapping-definition in class-mapping-definitions
       append
	 (loop for slot-mapping-definition
	    in (one-to-many-mappings-of class-mapping-definition)
	    for columns = (columns-of slot-mapping-definition)
	    when (eq mapped-class
		     (mapped-class-of slot-mapping-definition))
	    append (mapcar #'(lambda(column-name)
			       (make-instance 'column
					      :name column-name :sql-type
					      (get-reference-column-type
					       (position column-name columns)
					       class-mapping-definition)))
			   columns)))))

(defun get-primary-key-columns (class-mapping-definition)
  (mapcar #'(lambda (column-name)
	      (make-instance 'column :name column-name
			     :sql-type
			     (get-column-type column-name
					      class-mapping-definition)))
	  (primary-key-of class-mapping-definition)))

(defun get-superclasses-columns (class-mapping-definition)
  (loop for superclass in (superclasses-of class-mapping-definition)
       for superclass-mapping = (find-class-mapping-definition superclass)
       append (get-primary-key-columns superclass-mapping)))

(defun make-columns (class-mapping-definition)
  (remove-duplicates
   (append
    (get-value-mapping-columns class-mapping-definition)
    (get-superclasses-columns class-mapping-definition)
    (get-many-to-one-columns class-mapping-definition)
    (get-one-to-many-columns class-mapping-definition)
    :key #'name-of :test #'string=)))

(defun compute-tables (class-mapping-definitions)
    (mapcar #'(lambda (class-mapping-definition)
		(make-instance 'table :name
			       (table-name-of class-mapping-definition)
			       :columns (make-columns class-mapping-definition)))
	    class-mapping-definitions))

(defun make-class-mappings (class-mapping-definitions)
  (mapcar #'(lambda (class-mapping-definition)
	      (make-instance 'class-mapping :mapped-class
			     (mapped-class-of class-mapping-definition)))
	  class-mapping-definitions))

(defun make-one-to-many-association (slot-mapping-definition
				     class-mapping-definition)
  (let* ((many-to-one (get-class-mapping
		       (mapped-class-of class-mapping-definition)))
	 (table (table-of many-to-one)))
    (make-instance 'association
		   :many-to-one many-to-one
		   :one-to-many (get-class-mapping
				 (mapped-class-of slot-mapping-definition))
		   :columns (mapcar #'(lambda (column-name)
					(get-column column-name table))
				    (columns-of slot-mapping-definition)))))

(defun make-many-to-one-association (slot-mapping-definition
				     class-mapping-definition)
  (let* ((many-to-one (get-class-mapping
		       (mapped-class-of slot-mapping-definition)))
	 (table (table-of many-to-one)))
    (make-instance 'association
		   :many-to-one many-to-one
		   :one-to-many (get-class-mapping
				 (mapped-class-of class-mapping-definition))
		   :columns (mapcar #'(lambda (column-name)
					(get-column column-name table))
				    (columns-of slot-mapping-definition)))))

(defun make-associations (class-mapping-definition)
  (append
   (loop for slot-mapping
      in (many-to-one-mappings-of class-mapping-definition)
      for association = (make-many-to-one-association slot-mapping
						      class-mapping-definition)
     when (not (null association))
      collect association)
   (loop for slot-mapping
      in (one-to-many-mappings-of class-mapping-definition)
      for association = (make-one-to-many-association slot-mapping
						      class-mapping-definition)
      when (not (null association))
      collect association)))

(defun compute-associations (class-mapping-definitions)
  (remove-duplicates
   (mapcar #'make-associations class-mapping-definitions)
   :test #'equal
   :key #'(lambda (association)
	    (list (many-to-one-mapping-of association)
		  (one-to-many-mapping-of association)
		  (columns-of association)))))

(defun compute-mapping-presedence-list (mapped-class superclass)
  (list* superclass
	 (mapcar #'(lambda (superclass)
		     (when (eq mapped-class superclass)
		       (error "Superclass ~a inherit from subclass ~a"
			      superclass mapped-class))
		     (compute-mapping-presedence-list mapped-class
						      superclass)) 
		 (superclasses-of
		  (find-class-mapping-definition superclass)))))

(defun class-mapping-presedence-list (class-mapping)
  (let ((mapped-class (mapped-class-of class-mapping)))
    (sort (compute-mapping-presedence-list mapped-class mapped-class)
	  #'string< :key #'symbol-name)))

(defun symbol< (a b)
  (string< (symbol-name a) (symbol-name b)))

(defun presedence-list< (a b predicate)
  (if (not (null (symbol< (first a) (first b))))
      (presedence-list< (rest a) (rest b) predicate)))

(defun compute-inheritance-mapping (class-mapping-definition)
  (let* ((class-mapping
	  (get-class-mapping class-mapping-definition))
	 (table (table-of class-mapping)))
    (loop for superclass in (superclasses-of class-mapping-definition)
       collect (make-instance 'inheritance-mapping
			      :columns (mapcar #'(lambda (name)
						   (get-column name table))
					       (columns-of class-mapping-definition))
			      :superclass-mapping (get-class-mapping superclass)
			      :subclass-mapping class-mapping))))

(defun compute-inheritance-mappings (class-mapping-definitions)
  (apply #'append
	 (mapcar #'compute-inheritance-mappings
		 class-mapping-definitions)))

(defun compute-superclass-inheritance-mappings (class-mapping-definition)
  (remove (get-class-mapping
	   (mapped-class-of class-mapping-definition))
	  (list-inhritance-mappings)
	  :key #'subclass-mapping-of))

(defun compute-subclass-inheritance-mappings (class-mapping-definition)
  (remove (get-class-mapping
	   (mapped-class-of class-mapping-definition))
	  (list-inhritance-mappings)
	  :key #'superclass-mapping-of))

(defun compute-class-mapping (class-mapping class-mapping-definition)
  (with-slots (superclasses-inheritance-mappings
	       subclass-inheritance-mappings table)
      class-mapping
    (setf table
	  (get-table (table-name-of class-mapping-definition))
	  superclasses-inheritance-mappings
	  (compute-superclass-inheritance-mappings class-mapping-definition)
	  subclass-inheritance-mappings
	  (compute-subclass-inheritance-mappings class-mapping-definition))))

(defun compute-value-mappings (class-mapping-definition path)
  (let ((table (get-table (table-name-of class-mapping-definition))))
    (loop for slot-mapping-definition
       in (value-mappings-of class-mapping-definition)
       collect (with-slots (slot-name columns marshaller unmarshaller)
		   slot-mapping-definition
		 (make-instance 'value-mapping 
				:slot-name slot-name
				:path path
				:columns (mapcar #'(lambda (column-definition)
						     (get-column
						      (column-name-of column-definition)
						      table))
						 columns)
				:marshaller marshaller
				:unmarshaller unmarshaller)))))

(defun compute-one-to-many-mapping (slot-mapping-definition table path)
  (with-slots (slot-name columns marshaller unmarshaller referenced-class)
      slot-mapping-definition
    (make-instance 'many-to-one-mapping
		   :slot-name slot-name
		   :path path
		   :columns (mapcar #'(lambda (name)
					(get-column name table))
				    columns)
		   :marshaller marshaller
		   :unmarshaller unmarshaller
		   :referenced-class-mapping
		   (get-class-mapping referenced-class))))

(defun compute-many-to-one-mapping (slot-mapping-definition table path)
  (with-slots (slot-name columns marshaller unmarshaller referenced-class)
      slot-mapping-definition
    (make-instance 'one-to-many-mapping
		   :slot-name slot-name
		   :path path
		   :columns (mapcar #'(lambda (name)
					(get-column name table))
				    columns)
		   :marshaller marshaller
		   :unmarshaller unmarshaller
		   :referenced-class-mapping
		   (get-class-mapping referenced-class))))

(defun compute-reference-mappings (class-mapping-definition path)
  (let ((table (get-table (table-name-of class-mapping-definition))))
    (loop for slot-mapping-definition
       in (many-to-one-mappings-of class-mapping-definition)
       collect (compute-many-to-one-mapping slot-mapping-definition 
					    table path))
    (loop for slot-mapping-definition
       in (one-to-many-mappings-of class-mapping-definition)
       collect (compute-one-to-many-mapping slot-mapping-definition 
					  table path))))

(defun compute-slot-mappings (class-mapping &optional
			      (class-mapping-definition
			       (find-class-mapping-definition
				(mapped-class-of class-mapping))) path)
  (with-slots (value-mappings reference-mappings)
      class-mapping
    (setf value-mappings
	  (append value-mappings
		  (compute-value-mappings class-mapping-definition path))
	  reference-mappings
	  (append reference-mappings
		  (compute-reference-mappings class-mapping-definition path))))
  (dolist (inheritance-mapping (superclasses-inheritance-mappings-of 
				(get-class-mapping
				 (mapped-class-of class-mapping-definition))))
    (compute-slot-mappings class-mapping
			   (find-class-mapping-definition
			    (mapped-class-of
			     (superclass-mapping-of inheritance-mapping)))
			   (list* inheritance-mapping path))))

;; сначала созадем таблицы
;; затем создаем, но не инициализируем отображения
;; создаем асциации
;; теперь есть все, чтобы проинициализировать отображения
;; инициализируем в порядке наследования (presedence-list)

(defun make-mapping-schema (&rest class-mapping-definitions)
  (let* ((*class-mapping-definitions* class-mapping-definitions)
	 (tables
	  (compute-tables class-mapping-definitions))
	 (class-mappings
	  (make-class-mappings class-mapping-definitions))
	 (*mapping-schema*
	  (make-instance 'mapping-schema
			 :tables (alexandria:alist-hash-table
				  (mapcar #'(lambda (class-mapping)
					      (let ((table (table-of class-mapping)))
						(cons (name-of table)
						      table)))
					  tables))
			 :class-mappings (alexandria:alist-hash-table
					  (mapcar #'(lambda (class-mapping)
						      (cons
						       (mapped-class-of class-mapping)
						       class-mapping))
						  class-mappings)))))
    (with-slots (association-mappings inheritance-mappings)
	*mapping-schema*
      (setf association-mappings
	    (compute-associations class-mapping-definitions)
	    inheritance-mappings
	    (compute-inheritance-mappings class-mapping-definitions)))
    (dolist (class-mapping
	      (sort class-mappings #'presedence-list<
		    :key #'class-mapping-presedence-list)
	     *mapping-schema*)
      (compute-class-mapping class-mapping
			     (find-class-mapping-definition
			      (mapped-class-of class-mapping)))
      (compute-slot-mappings class-mapping))))
