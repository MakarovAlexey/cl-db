;;; cl-db.lisp

(in-package #:cl-db)

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

(defun compute-mapping (class-mapping root-name &rest class-mappings)
  (parse-mapping #'(lambda (&key class-name primary-key &allow-other-keys)
		     (list*
		      (apply #'compute-inheritance class-mapping
			     root-name class-mappings)
		      (apply #'compute-subclass-mappings class-name
			     class-mappings)))
		 class-mapping))

(defmacro define-schema (name params &body class-mappings)
  (declare (ignore params))
  `(defun ,name ()
     (list
      (quote ,(mapcar #'(lambda (class-mapping)
			   (apply #'compute-mapping
				  class-mapping nil class-mappings))
		       class-mappings)))))

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
