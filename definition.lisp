(in-package #:cl-db)

(defvar *mappings* (make-hash-table))

(defmacro define-mapping (name)
  (setf (gethash name *mappings*)
	(make-instance 'mapping-definition)))

(defvar *mapping-definition*)

(defclass mapping-definition ()
  ((class-mappings :initform (make-hash-table)
		   :accessor class-mappings-of))
  (:metaclass closer-mop:funcallable-standard-class))

(defun find-class-mapping (class)
  (gethash class (class-mappings-of *mapping-definition*)))

(defun list-class-mappings ()
  (alexandria:hash-table-values
   (class-mappings-of *mapping-definition*)))

(defmacro use-mapping (name)
  (setf *mapping-definition*
	(gethash name *mappings*)))

(define-condition class-mapping-redefinition (style-warning)
  ((mapped-class :initarg :mapped-class :reader mapped-class)))

(defclass class-mapping-definition ()
  ((mapped-class :initarg :mapped-class
		 :reader mapped-class-of)
   (table-name :initarg :table-name
	       :reader table-name-of)
   (table-symbol :initform (alexandria:ensure-symbol (gensym "TABLE-"))
		 :reader table-symbol-of)
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
  ((slot-name :initarg :slot-name :reader slot-name-of)
   (columns :initarg :columns :reader columns-of)
   (unmarshaller :initarg :unmarshaller :reader unmarshaller-of)
   (marshaller :initarg :marshaller :reader marshaller-of)))

(defclass value-mapping-definition
    (slot-mapping-definition)
  ())

(defclass reference-mapping-definition (slot-mapping-definition)
  ((mapped-class :initarg :mapped-class :reader mapped-class-of)))

(defclass many-to-one-mapping-definition
    (reference-mapping-definition)
  ())

(defclass one-to-many-mapping-definition
    (reference-mapping-definition)
  ())

(defun reference-class-mapping (reference-mapping)
  (find-class-mapping (mapped-class-of reference-mapping)))

(defun compile-option (option &rest options)
  (rest (assoc option options)))

(defun make-value-mapping (slot-name marshaller unmarshaller
			   column &rest columns)
  (make-instance 'value-mapping-definition
		 :slot-name slot-name
		 :columns (list* column columns)
		 :marshaller marshaller
		 :unmarshaller unmarshaller))

(defun make-many-to-one-mapping (slot-name marshaller unmarshaller
				 mapped-class column &rest columns)
  (make-instance 'many-to-one-mapping-definition
		 :slot-name slot-name
		 :mapped-class mapped-class
		 :columns (list* column columns)
		 :marshaller marshaller
		 :unmarshaller unmarshaller))

(defun make-one-to-many-mapping (slot-name marshaller unmarshaller
				 mapped-class column &rest columns)
  (make-instance 'one-to-many-mapping-definition
		 :slot-name slot-name
		 :mapped-class mapped-class
		 :columns (list* column columns)
		 :marshaller marshaller
		 :unmarshaller unmarshaller))

(defun compile-slot-mappings (type function &rest slot-mappings)
  (let ((mappings (list)))
    (dolist (slot-mapping slot-mappings)
      (destructuring-bind
	    (slot-name (mapping-type &rest options)
		       &optional marshaller unmarshaller)
	  slot-mapping
	(when (eq mapping-type type)
	  (push (apply function slot-name
		       marshaller unmarshaller options)
		mappings))))
    (reverse mappings)))

(defmacro define-class-mapping ((class-name table-name) options
				&rest slot-mappings)
  (let ((mapped-class class-name))
    (multiple-value-bind (mapping presentp)
	(gethash mapped-class (class-mappings-of *mapping-definition*))
      (declare (ignore mapping))
      (when presentp
	(warn 'class-mapping-redefinition :mapped-class mapped-class)))
    (setf (gethash mapped-class (class-mappings-of *mapping-definition*))
	  (make-instance 'class-mapping-definition
			 :mapped-class mapped-class
			 :table-name table-name
			 :primary-key (apply #'compile-option
					     :primary-key options)
			 :superclasses (apply #'compile-option
					      :superclasses options)
			 :value-mappings (apply #'compile-slot-mappings
						:value
						#'make-value-mapping
						slot-mappings)
			 :many-to-one-mappings (apply #'compile-slot-mappings
						      :many-to-one
						      #'make-many-to-one-mapping
						      slot-mappings)
			 :one-to-many-mappings (apply #'compile-slot-mappings
						      :one-to-many
						      #'make-one-to-many-mapping
						      slot-mappings)))))

(defun compile-value-mapping-columns (value-mapping)
  (loop for (name type-name) in (columns-of value-mapping)
     collect `(make-instance 'column :name ,name
			     :type-name ,type-name)))

(defgeneric find-column-type (column-name slot-mapping))

(defmethod find-column-type (column-name
			     (slot-mapping value-mapping-definition))
  (let ((column (assoc column-name (columns-of slot-mapping)
		       :test #'equal)))
    (when (not (null column))
      (destructuring-bind (column-name column-type) column
	(declare (ignore column-name))
	column-type))))

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
  (let ((column-type (loop for value-mapping
			in (value-mappings-of class-mapping-definition)
			thereis (find-column-type column-name value-mapping))))
    (if (null column-type)
	(loop for many-to-one-mapping
	   in (many-to-one-mappings-of class-mapping-definition)
	   thereis (find-column-type column-name many-to-one-mapping))
	column-type)))

(defun get-reference-column-type (column-position mapped-class)
  (let* ((class-mapping-definition (find-class-mapping mapped-class))
	 (primary-key (primary-key-of class-mapping-definition))
	 (superclasses (superclasses-of class-mapping-definition)))
    (if (not (null (or primary-key superclasses)))
	(if (not (null primary-key))
	    (get-column-type (elt primary-key column-position)
			     class-mapping-definition)
	    (get-reference-column-type column-position
				       (first superclasses)))
	(error "For mapping of class ~a not specified primary key or superclasses"
	       (class-name mapped-class)))))

(defun get-value-mapping-columns (class-mapping-definition)
  (loop for value-mapping
     in (value-mappings-of class-mapping-definition)
     append (columns-of value-mapping)))

(defun get-many-to-one-columns (class-mapping-definition)
  (loop for many-to-one-mapping
     in (many-to-one-mappings-of class-mapping-definition)
     for columns = (columns-of many-to-one-mapping)
     append (loop for column-name in columns
	       collect (list column-name
			     (get-reference-column-type
			      (position column-name columns)
			      (mapped-class-of many-to-one-mapping))))))

(defun get-one-to-many-columns (mapped-class many-to-one-columns)
  (loop for class-mapping-definition
     in (list-class-mappings) append
       (loop for one-to-many
	  in (one-to-many-mappings-of class-mapping-definition)
	  for columns = (columns-of one-to-many)
	  when (eq mapped-class (mapped-class-of one-to-many))
	  append (loop for column-name in columns
		    when (not (assoc column-name many-to-one-columns
				     :test #'equal))
		    collect (list column-name
				  (get-reference-column-type
				   (position column-name columns)
				   (mapped-class-of class-mapping-definition)))))))

(defun get-columns (class-mapping-definition)
  (let* ((value-columns
	  (get-value-mapping-columns class-mapping-definition))
	 (superclasses-columns
	  (get-superclasses-columns class-mapping-definition))
	 (many-to-one-columns
	  (get-many-to-one-columns class-mapping-definition))
	 (one-to-many-columns
	  (get-one-to-many-columns
	   (mapped-class-of class-mapping-definition)
	   (append superclasses-columns many-to-one-columns))))
    (append value-columns
	    superclasses-columns
	    many-to-one-columns
	    one-to-many-columns)))

(defun get-primary-key (class-mapping-definition)
  (or (primary-key-of class-mapping-definition)
      (get-primary-key
       (find-class-mapping
	(first (superclasses-of class-mapping-definition))))))

(defun get-primary-key-columns (class-mapping-definition)
  (let ((columns (get-columns class-mapping-definition)))
    (mapcar #'(lambda (column)
		(assoc column columns :test #'equal))
	    (get-primary-key class-mapping-definition))))

(defun get-superclasses-columns (class-mapping-definition)
  (reduce #'(lambda (columns superclass-mapping)
	      (union columns
		     (get-primary-key-columns superclass-mapping)
		     :test #'equal))
	  (mapcar #'find-class-mapping
		  (superclasses-of class-mapping-definition))
	  :initial-value nil))

(defun compile-columns (class-mapping-definition)
  (loop for (column-name column-type)
     in (get-columns class-mapping-definition)
     collect `(make-instance 'column
			     :name ,column-name
			     :type-name ,column-type)))

(defun compile-table (class-mapping-definition)
  `(,(table-symbol-of class-mapping-definition)
     (make-instance 'table
		    :name ,(table-name-of class-mapping-definition)
		    :primary-key (quote
				  ,(get-primary-key class-mapping-definition))
		    :columns (list
			      ,@(compile-columns class-mapping-definition)))))

(defun compile-tables (&rest class-mapping-definitions)
  (mapcar #'compile-table class-mapping-definitions))

(defun make-superclass-foreign-key-name (class-mapping
					 superclass-mapping)
  (format nil "fk_~a_inheritance_~a"
	  (table-name-of class-mapping)
	  (table-name-of superclass-mapping)))

(defun get-superclasses-foreign-keys (class-mapping-definition)
  (mapcar #'(lambda (superclass)
	      (let ((superclass-mapping
		     (find-class-mapping superclass)))
	      `(,(make-superclass-foreign-key-name
		  class-mapping-definition
		  superclass-mapping)
		 ,(table-symbol-of class-mapping-definition)
		 ,(table-symbol-of superclass-mapping)
		 ,@(get-primary-key superclass-mapping))))
	  (superclasses-of class-mapping-definition)))

(defun make-foreign-key-symbol (name)
  (alexandria:ensure-symbol
   (make-symbol (string-upcase name))))

(defun compile-foreign-key (name table referenced-table
			    column &rest columns)
  `(,(make-foreign-key-symbol name)
     (make-instance 'foreign-key
		    :name ,name
		    :table ,table
		    :referenced-table ,referenced-table
		    :columns (quote ,(list* column columns)))))

(defun compile-superclass-foreign-keys (&rest class-mapping-definitions)
  (mapcar #'(lambda (foreign-key)
	      (apply #'compile-foreign-key foreign-key))
	  (loop for class-mapping-definition
	     in class-mapping-definitions
	     append (get-superclasses-foreign-keys class-mapping-definition))))

(defun make-many-to-one-foreign-key-name (class-mapping-definition
					  many-to-one-mapping)
  (format nil "fk_~a_reference_~a"
	  (table-name-of class-mapping-definition)
	  (string-downcase
	   (string (slot-name-of many-to-one-mapping)))))

(defun bidirectional-p (mapped-class many-to-one-mapping)
  (let ((columns (columns-of many-to-one-mapping)))
    (loop for one-to-many-mapping
       in (one-to-many-mappings-of
	   (reference-class-mapping many-to-one-mapping))
       thereis (and (eq (mapped-class-of one-to-many-mapping)
			mapped-class)
		    (equal (columns-of one-to-many-mapping)
			   columns)))))

(defun get-many-to-one-foreign-keys (class-mapping-definition)
  (loop for many-to-one-mapping
     in (many-to-one-mappings-of class-mapping-definition)
     when (not (bidirectional-p
		(mapped-class-of class-mapping-definition)
		many-to-one-mapping))
       collect (let ((reference-class-mapping
		     (reference-class-mapping many-to-one-mapping)))
		`(,(make-many-to-one-foreign-key-name
		    class-mapping-definition
		    many-to-one-mapping)
		   ,(table-symbol-of class-mapping-definition)
		   ,(table-symbol-of reference-class-mapping)
		   ,@(columns-of many-to-one-mapping)))))

(defun make-one-to-many-foreign-key-name (class-mapping-definition
					  one-to-many-mapping)
  (format nil "fk_~a_~a_~a"
	  (table-name-of
	   (reference-class-mapping one-to-many-mapping))
	  (string-downcase
	   (string (mapped-class-of class-mapping-definition)))
	  (string-downcase
	   (string (slot-name-of one-to-many-mapping)))))

(defun get-one-to-many-foreign-keys (class-mapping-definition)
  (loop for one-to-many-mapping
     in (one-to-many-mappings-of class-mapping-definition)
     collect (let ((reference-class-mapping
		    (reference-class-mapping one-to-many-mapping)))
	       `(,(make-one-to-many-foreign-key-name
		   class-mapping-definition
		   one-to-many-mapping)
		  ,(table-symbol-of reference-class-mapping)
		  ,(table-symbol-of class-mapping-definition)
		  ,@(columns-of one-to-many-mapping)))))

(defun get-reference-foreign-keys (class-mapping-definition)
  (append
   (get-one-to-many-foreign-keys class-mapping-definition)
   (get-many-to-one-foreign-keys class-mapping-definition)))

(defun compile-reference-foreign-keys (&rest class-mapping-definitions)
  (mapcar #'(lambda (foreign-key)
	      (apply #'compile-foreign-key foreign-key))
	  (loop for class-mapping-definition
	     in class-mapping-definitions append
	       (get-reference-foreign-keys class-mapping-definition))))

(defmacro compile-mapping ()
  (let* ((class-mapping-definitions (list-class-mappings))
	 (tables
	  (apply #'compile-tables class-mapping-definitions))
	 (superclass-foreign-keys
	  (apply #'compile-superclass-foreign-keys
		 class-mapping-definitions))
	 (reference-foreign-keys
	  (apply #'compile-reference-foreign-keys
		 class-mapping-definitions)))
    `(let* (,@tables
	    ,@superclass-foreign-keys
	    ,@reference-foreign-keys)
       (closer-mop:set-funcallable-instance-function
	*mapping-definition*
	#'(lambda ()
	    (make-instance 'database-schema
			   :tables (list ,@(mapcar #'first tables))
			   :foreign-keys
			   (list ,@(mapcar #'first
					   superclass-foreign-keys))))))))