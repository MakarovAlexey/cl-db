;; implement (or class-name null) - type form
;; implement type declaration for values

(in-package #:cl-db)

(defun append-class-mapping-definition (mapping-schema
					class-mapping-definition)
  (make-mapping-schema
   (list* class-mapping-definition
	  (class-mapping-definitions-of mapping-schema))))



(define-condition class-mapping-redefinition (style-warning)
  ((mapped-class :initarg :mapped-class :reader mapped-class)))

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

(defmacro define-class-mapping (name (class-name table-name) options
				&body slot-mappings)
  `(append-class-mapping-definition *mapping-schema* ;; проверить на наличие *mapping-schema*
     (make-instance 'class-mapping-definition
		    :mapped-class (find-class ,class-name)
		    :table-name ,table-name
		    :primary-key (quote
				  ,(apply #'compile-option
					  :primary-key options))
		    :superclasses (mapcar #'find-class
					  (quote ,(apply #'compile-option
							 :superclasses options)))
		    :slot-mappings (quote
				    ,(compile-slot-mappings slot-mappings)))))

(defun sql-name (string)
  (substitute #\_ #\- (string-downcase string)))

(defun lisp-name (string)
  (substitute #\- #\_ (string-upcase string)))