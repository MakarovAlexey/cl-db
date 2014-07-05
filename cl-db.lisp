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

(defun compute-subclass-mappings (class-name primary-key &rest class-mappings)
  (mapcar #'(lambda (subclass-mapping)
	      (list*
	       (apply #'compute-mapping subclass-mapping
		      class-name class-mappings)
	       (parse-mapping
		#'(lambda (&key superclasses &allow-other-keys)
		    (assoc class-name superclasses))
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
		     (list :class-name class-name
			   :table-name table-name
			   :primary-key primary-key
			   :superclasses
			   (mapcar #'(lambda (superclass-mapping)
				       (destructuring-bind (class-name &rest columns)
					   superclass-mapping
					 (list*
					  (apply #'compute-inheritance
						 (assoc class-name class-mappings)
						 class-mappings)
					  columns)))
				   (remove root-name superclasses :key #'first))))
		 class-mapping))



(defun compute-class-mappings (class-mapping &rest class-mappings)
  (destructuring-bind
	(class-name ((table-name &rest primary-key)
		     &rest superclasses)
		    &rest slot-mappings) class-mapping
    (list* (list class-name table-name primary-key)
	   (when (not (null class-mappings))
	     (apply #'compute-class-mappings class-mappings)))))

(defun compute-superclass-mappings (class-mapping &rest class-mappings)
  (destructuring-bind
	(class-name ((table-name &rest primary-key)
		     &rest superclasses)
		    &rest slot-mappings) class-mapping
    (append
     (reduce #'(lambda (superclass-mappings superclass-mapping)
		 (destructuring-bind (superclass-name &rest foreign-key)
		     superclass-mapping
		   (list*
		    (list class-name superclass-name foreign-key)
		    superclass-mappings)))
	     superclasses
	     :initial-value nil)
     (when (not (null class-mappings))
       (apply #'compute-superclass-mappings class-mappings)))))

(defun compute-value-mappings (class-mapping &rest class-mappings)
  (list*
   (destructuring-bind
	 (class-name ((table-name &rest primary-key)
		      &rest superclasses)
		     &rest slot-mappings) class-mapping
     (list class-name 
	   (reduce #'(lambda (slot-mappings slot-mapping)
		       (destructuring-bind
			     (slot-name (mapping-type &rest args)
					&optional deserializer serializer)
			   slot-mapping
			 (if (eq mapping-type :property)
			     (list*
			      (list slot-name args deserializer serializer)
			      slot-mappings)
			     slot-mappings)))
		   slot-mappings
		   :initial-value nil)))
   (when (not (null class-mappings))
     (apply #'compute-value-mappings class-mappings))))

(defmacro define-schema (name params &body class-mappings)
  (declare (ignore params))
  `(defun ,name ()
     (list :class-mappngs
	   (quote ,(apply #'compute-class-mappings class-mappings))
	   :superclass-mappings
	   (quote ,(apply #'compute-superclass-mappings class-mappings))
	   :value-mappings
	   (quote ,(apply #'compute-value-mappings class-mappings)))))

;;      (quote ,(mapcar #'(lambda (class-mapping)
;;			   (apply #'compute-mapping
;;				  class-mapping nil class-mappings))
;;		       class-mappings)))))

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
