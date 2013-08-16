;; implement (or class-name null) - type form
;; implement type declaration for values

(in-package #:cl-db)

(defvar *mappings* (make-hash-table))

(defmacro define-mapping (name)
  (setf (gethash name *mappings*)
	(make-instance 'mapping-definition :name name)))

(defun get-mapping-definition (name)
  (gethash name *mappings*))

(defvar *mapping-definition*)

(defmacro use-mapping (name)
  (setf *mapping-definition*
	(get-mapping-definition name)))

(defclass mapping-definition ()
  ((name :initarg :name
	 :reader name-of)
   (class-mappings :initform (make-hash-table)
		   :accessor class-mappings-of))
  (:metaclass closer-mop:funcallable-standard-class))

(defun find-class-mapping (class)
  (multiple-value-bind (mapping presentp)
      (gethash class (class-mappings-of *mapping-definition*))
    (when (not presentp)
      (error "Mapping for class ~a not defined"
	     class))
    mapping))

(defun list-class-mappings ()
  (alexandria:hash-table-values
   (class-mappings-of *mapping-definition*)))

(define-condition class-mapping-redefinition (style-warning)
  ((mapped-class :initarg :mapped-class :reader mapped-class)))

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

(defun sql-name (string)
  (substitute #\_ #\- (string-downcase string)))

(defun lisp-name (string)
  (substitute #\- #\_ (string-upcase string)))

(defun table-symbol (class-mapping-definition)
  (alexandria:ensure-symbol
    (lisp-name
     (format nil "~a-table"
	     (table-name-of class-mapping-definition)))))

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
	       mapped-class))))

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
			     :sql-type ,column-type)))

(defun compile-table (class-mapping-definition)
  `(,(table-symbol class-mapping-definition)
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
  (sql-name
   (format nil "fk_~a_inheritance_~a"
	   (table-name-of class-mapping)
	   (table-name-of superclass-mapping))))

(defun get-superclasses-foreign-keys (class-mapping-definition)
  (mapcar #'(lambda (superclass)
	      (let ((superclass-mapping
		     (find-class-mapping superclass)))
	      `(,(make-superclass-foreign-key-name
		  class-mapping-definition
		  superclass-mapping)
		 ,(table-symbol class-mapping-definition)
		 ,(table-symbol superclass-mapping)
		 ,@(get-primary-key superclass-mapping))))
	  (superclasses-of class-mapping-definition)))

(defun make-foreign-key-symbol (name)
  (alexandria:ensure-symbol
   (make-symbol (lisp-name name))))

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
  (sql-name
   (format nil "fk_~a_~a_~a"
	   (table-name-of class-mapping-definition)
	   (table-name-of
	    (reference-class-mapping many-to-one-mapping))
	   (slot-name-of many-to-one-mapping))))

(defun make-one-to-many-foreign-key-name (class-mapping-definition
					  one-to-many-mapping)
  (sql-name
   (format nil "fk_~a_~a_~a"
	   (table-name-of
	    (reference-class-mapping one-to-many-mapping))
	   (table-name-of class-mapping-definition)
	   (slot-name-of one-to-many-mapping))))

(defun get-one-to-many-mapping (mapped-class many-to-one-mapping)
  (let ((columns (columns-of many-to-one-mapping)))
    (find-if #'(lambda (one-to-many-mapping)
		 (and (eq (mapped-class-of one-to-many-mapping)
			  mapped-class)
		      (equal (columns-of one-to-many-mapping)
			     columns)))
	     (one-to-many-mappings-of
	      (reference-class-mapping many-to-one-mapping)))))

(defun bidirectional-p (mapped-class many-to-one-mapping)
  (not (null (get-one-to-many-mapping mapped-class
				      many-to-one-mapping))))

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
		   ,(table-symbol class-mapping-definition)
		   ,(table-symbol reference-class-mapping)
		   ,@(columns-of many-to-one-mapping)))))

(defun get-one-to-many-foreign-keys (class-mapping-definition)
  (loop for one-to-many-mapping
     in (one-to-many-mappings-of class-mapping-definition)
     collect (let ((reference-class-mapping
		    (reference-class-mapping one-to-many-mapping)))
	       `(,(make-one-to-many-foreign-key-name
		   class-mapping-definition
		   one-to-many-mapping)
		  ,(table-symbol reference-class-mapping)
		  ,(table-symbol class-mapping-definition)
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

(defun compile-value-mappings (class-mapping-definition)
  (mapcar #'(lambda (value-mapping)
	      (with-slots (slot-name columns marshaller unmarshaller)
		  value-mapping
	      `(make-instance 'value-mapping
			      :slot-name (quote ,slot-name)
			      :marshaller ,marshaller
			      :unmarshaller ,unmarshaller
			      :columns (quote ,(mapcar #'first
						      columns)))))
	  (value-mappings-of class-mapping-definition)))

(defun get-class-mapping-name (mapped-class)
  (alexandria:ensure-symbol
   (lisp-name (format nil "~a-mapping" mapped-class))))

(defun compile-reference-mapping (mapping-type fk-name mapping)
  (with-slots (slot-name columns mapped-class marshaller unmarshaller)
      mapping
    `(cons (quote ,slot-name)
	   (make-instance (quote ,mapping-type)
			  :slot-name (quote ,slot-name)
			  :class-mapping ,(get-class-mapping-name mapped-class)
			  :foreign-key ,fk-name
			  :marshaller ,marshaller
			  :unmarshaller ,unmarshaller))))

(defun compile-reference-mappings (class-mapping-definition)
  (let ((mapped-class
	 (mapped-class-of class-mapping-definition))
	(class-mapping-bind-name
	 (get-class-mapping-name
	  (mapped-class-of class-mapping-definition))))
    `((reference-mappings-of ,class-mapping-bind-name)
      (alexandria:alist-hash-table
       (list
	,@(mapcar #'(lambda(mapping)
		      (let ((one-to-many-mapping
			     (get-one-to-many-mapping mapped-class mapping)))
			(compile-reference-mapping
			 'many-to-one-mapping
			 (make-foreign-key-symbol
			  (if (not (null one-to-many-mapping))
			      (make-one-to-many-foreign-key-name
			       (find-class-mapping (mapped-class-of mapping))
			       one-to-many-mapping)
			      (make-many-to-one-foreign-key-name
			       class-mapping-definition
			       mapping)))
			 mapping)))
		  (many-to-one-mappings-of class-mapping-definition))
	,@(mapcar #'(lambda(mapping)
		      (compile-reference-mapping
		       'one-to-many-mapping
		       (make-foreign-key-symbol
			(make-one-to-many-foreign-key-name
			 class-mapping-definition
			 mapping))
		       mapping))
		  (one-to-many-mappings-of class-mapping-definition)))))))

(defun compile-superclass-mappings (subclass-mapping-definition)
  `((superclasses-mappings-of
     ,(get-class-mapping-name
       (mapped-class-of subclass-mapping-definition)))
    (list ,@(mapcar #'(lambda (mapped-class)
			`(make-instance 'superclass-mapping
					:class-mapping ,(get-class-mapping-name
							 mapped-class)
					:foreign-key ,(make-foreign-key-symbol
						       (make-superclass-foreign-key-name
							subclass-mapping-definition
							(find-class-mapping mapped-class)))))
		    (superclasses-of subclass-mapping-definition)))))

(defun compile-subclass-mappings (superclass-mapping-definition)
  (let ((class-name
	 (mapped-class-of superclass-mapping-definition)))
    `((subclasses-mappings-of
       ,(get-class-mapping-name class-name))
      (list ,@(loop for class-mapping-definition in (list-class-mappings)
		 when (not
		       (null (find class-name
				   (superclasses-of class-mapping-definition))))
		 collect
		   `(make-instance 'subclass-mapping
				   :class-mapping ,(get-class-mapping-name
						    (mapped-class-of class-mapping-definition))
				   :foreign-key ,(make-foreign-key-symbol
						  (make-superclass-foreign-key-name
						   class-mapping-definition
						   superclass-mapping-definition))))))))

(defun compile-class-mapping (class-mapping-definition)
  (with-slots (mapped-class) class-mapping-definition
    `(,(get-class-mapping-name mapped-class)
       (make-instance 'class-mapping
		      :mapped-class (find-class
				     (quote ,mapped-class))
		      :table ,(table-name-of class-mapping-definition)
		      :value-mappings (list
				       ,@(compile-value-mappings
					  class-mapping-definition))))))

(defun compile-class-mappings (&rest class-mapping-definitions)
  (mapcar #'compile-class-mapping
	  class-mapping-definitions))

(defmacro compile-mapping (name)
  (let* ((*mapping-definition* (get-mapping-definition name))
	 (class-mapping-definitions (list-class-mappings))
	 (tables
	  (apply #'compile-tables class-mapping-definitions))
	 (superclass-foreign-keys
	  (apply #'compile-superclass-foreign-keys
		 class-mapping-definitions))
	 (reference-foreign-keys
	  (apply #'compile-reference-foreign-keys
		 class-mapping-definitions))
	 (class-mappings
	  (apply #'compile-class-mappings
		 class-mapping-definitions)))
    `(defun ,(name-of *mapping-definition*) ()
       (let* (,@tables
	      ,@superclass-foreign-keys
	      ,@reference-foreign-keys
	      ,@class-mappings)
	 (setf ,@(loop for cmd in class-mapping-definitions
		    append (compile-reference-mappings cmd))
	       ,@(loop for cmd in class-mapping-definitions
		    append (compile-superclass-mappings cmd))
	       ,@(loop for cmd in class-mapping-definitions
		    append (compile-subclass-mappings cmd)))
       ;;       (closer-mop:set-funcallable-instance-function
       ;;	*mapping-definition*
	     (make-instance 'mapping-schema
			    :tables (list ,@(mapcar #'first tables))
			    :class-mappings (list
					     ,@(mapcar #'first
						       class-mappings)))))))

(defun compile-function (name lambda-expression)
  (destructuring-bind (function-symbol
		       (lambda-symbol (&rest params)
				      &rest function-body))
      lambda-expression
    (declare (ignore function-symbol lambda-symbol))
    `(defun ,name ,params ,function-body)))