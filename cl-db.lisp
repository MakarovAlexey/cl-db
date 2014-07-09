;;; cl-db.lisp

(in-package #:cl-db)

(defclass direct-class-mapping ()
  ((class-name :initarg :class-name
	       :reader class-name-of)
   (table-name :initarg :class-name
	       :reader table-name-of)
   (primary-key :initarg :foreign-key
		:reader foreign-key)
   (superclass-mappings :initarg :superclass-mappings
			:reader superclass-mappings-of)
   (value-mappings :initarg :value-mappings
		   :reader value-mappings-of)
   (reference-mappings :initarg :reference-mappings
		       :reader reference-mappings-of)))

(defclass inheritance-mapping ()
  ((class-name :initarg :class-name
	       :reader class-name-of)
   (superclass-mapping :initarg :superclass-name
		       :reader superclass-name-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key)))

(defclass slot-mapping ()
  ((slot-name :initarg :class-name
	      :reader slot-name-of)
   (direct-class-name :initarg :direct-class-name
		      :reader direct-class-name-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns
	    :reader columns-of)
   (serializer :initarg :serializer
	       :reader serializer-of)
   (deserializer :initarg :deserializer
		 :reader deserializer-of)))

(defclass reference-mapping (slot-mapping)
  ((referenced-class-name :initarg :referenced-class-name
			  :reader referenced-class-name-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass many-to-one-mapping (reference-mapping)
  ())

(defclass one-to-many-mapping (reference-mapping)
  ((serializer :initarg :serializer
	       :reader serializer-of)
   (deserializer :initarg :deserializer
		 :reader deserializer-of)))

(defclass effective-class-mapping (direct-class-mapping)
  ((mapping-presedence-list :initarg :mapping-presedence-list
			    :reader mapping-presedence-list-of)
   (subclass-mappings :initarg :subclass-mappings
		      :reader subclass-mappings-of)))

(defun compute-class-mapping (class-mapping &rest class-mappings)
  (destructuring-bind
	(class-name ((table-name &rest primary-key)
		     &rest superclasses)
		    &rest slot-mappings) class-mapping
    (let ((superclass-mappings
	   (mapcar #'(lambda (superclass-mapping)
		       (apply #'compute-superclass-mapping
			      superclass-mapping
			      class-mappings))
		   superclasses)))
      (list* :class-name class-name
	     :table-name table-name
	     :primary-key primary-key
	     :superclass-mapping superclass-mappings
	     :superclass-presedence-list (list)
	     :subclass (list)
	     (apply #'compute-slot-mappings slot-mappings)))))
	     

(defun compute-mappings (&rest class-mappings)
  (let ((direct-class-mappings
	 (apply #'compute-direct-class-mappings class-mappings)))
    (mapcar #'(lambda (direct-class-mapping)
		(apply #'compute-effective-class-mapping
		       direct-class-mapping direct-class-mappings))
	    direct-class-mappings)))


(defmacro define-schema (name params &body class-mappings)
  (declare (ignore params))
  `(defun ,name ()
     (list
      (quote ,(mapcar #'(lambda (class-mapping)
			   (apply #'compute-mapping
				  class-mapping nil class-mappings))
		       class-mappings)))))

(defun parse-mapping (function class-mapping)
  (destructuring-bind
	(class-name ((table-name &rest primary-key)
		     &rest superclasses)
		    &rest slot-mappings) class-mapping
    (funcall function
	     :class-name class-name
	     :table-name table-name
	     :primary-key primary-key
	     :superclasses superclasses
	     :slot-mappings slot-mappings)))

(defun compute-subclass-mappings (class-name &rest class-mappings)
  (mapcar #'(lambda (subclass-mapping)
	      (list*
	       (apply #'compute-mapping subclass-mapping
		      class-name class-mappings)
	       (parse-mapping
		#'(lambda (&key superclasses &allow-other-keys)
		    (rest (assoc class-name superclasses)))
		subclass-mapping)))
	  (remove-if-not #'(lambda (subclass-mapping)
			     (parse-mapping
			      #'(lambda (&key superclasses &allow-other-keys)
				  (find class-name superclasses
					:key #'first))
			      subclass-mapping))
			 class-mappings)))

(defun compute-inheritance (class-mapping root-name &rest class-mappings)
  (parse-mapping #'(lambda (&key class-name table-name primary-key
			      superclasses &allow-other-keys)
		     (list* class-name
			    table-name primary-key
			    (mapcar #'(lambda (superclass-mapping)
					(destructuring-bind (class-name &rest columns)
					    superclass-mapping
					  (list*
					   (apply #'compute-inheritance
						  (assoc class-name class-mappings)
						  nil
						  class-mappings)
					   columns)))
				    (remove root-name superclasses :key #'first))))
		 class-mapping))




;;(table-name alias (&rest primary-key)
;;	    (class-name (((&rest superclass-mappings)
;;			  &rest value-mappings)
;;			 &rest reference-mappings)
;;			&rest subclass-mappings))
;;reference-mappigs => (slot-name (query-modifier &rest columns)
;;			   deserializer
;;			   serializer)

;; (defvar *mapping-schema-definitions* (make-hash-table))

;; (defvar *class-mapping-definitions*)

;; (defmacro define-mapping-schema (name)
;;  `(ensure-mapping-schema-definition (quote ,name)))

;;(defmacro define-class-mapping ((class-name table-name)
;;				options	&body slot-mappings)
;;  (assert-duplicate-slot-mappings slot-mappings)
;;  `(ensure-class-mapping-definition
;;    (find-class (quote ,class-name))
;;    ,table-name
;;    (quote ,(compile-option :primary-key options))
;;    (mapcar #'find-class
;;	    (quote ,(compile-option :superclasses options)))
;;    (list
;;     ,@(apply #'compile-slot-mappings :value
;;	      #'make-value-mapping slot-mappings))
;;    (list
;;     ,@(apply #'compile-slot-mappings :many-to-one
;;	      #'make-many-to-one-mapping slot-mappings))
;;    (list
;;     ,@(apply #'compile-slot-mappings :one-to-many
;;	      #'make-one-to-many-mapping slot-mappings))))
