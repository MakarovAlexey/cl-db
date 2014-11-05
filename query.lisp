(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias &rest column-names)
  (mapcar #'(lambda (column-name)
	      (list alias column-name))
	  column-names))

(defun append-join (table-join &rest super-slot-mappings)
  (reduce #'(lambda (result mapping)
	      (destructuring-bind (slot-definition . mapping)
		  mapping
		(acons slot-definition
		       #'(lambda ()
			   (multiple-value-bind
				 (columns loader from-clause)
			       (funcall function)
			     (values columns loader
				     (list* table-join from-clause))))
		       result)))
	  super-slot-mappings :initial-value slot-mappings))

(defun plan-properties (alias &rest properties)
  (reduce #'(lambda (result property)
	      (destructuring-bind (slot-definition column-name)
		  property
		(let* ((column
			(concatenate 'string alias "." column-name))
		       (loader #'(lambda (row)
				   (rest (assoc column row)))))
		  (acons slot-definition
			 #'(lambda ()
			     (values
			      (list alias column-name)
			      #'(lambda (object &rest row)
				  (setf
				   (slot-value object 
					       (slot-definition-name slot-definition))
				   (rest (assoc slot-definition row))))))
			 result))))
	  properties :initial-value nil))

(defun plan-many-to-one (slot-definition class-name &rest foreign-key)
  (destructuring-bind (class-name &key table-name primary-key
				  properties one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
      (assoc class-name *mapping-schema* :key #'first)
    (let* ((alias (make-alias))
	   (table-join
	    (list :left-join table-name alias
		  (mapcar #'append foreign-key
			  (apply #'append-alias alias primary-key)))))
      (plan-class-mapping alias table-join primary-key properties
			  one-to-many-mappings many-to-one-mappings
			  superclass-mappings subclass-mappings))))

(defun plan-many-to-one-mappings (alias &rest many-to-one-mappings)
  (reduce #'(lambda (result many-to-one-mapping)
	      (destructuring-bind (slot-definition
				   &key class-name foreign-key)
		  many-to-one-mapping
		(acons slot-definition
		       #'(lambda ()
			   (apply #'plan-many-to-one
				  slot-definition class-name
				  (apply #'append-alias
					 alias foreign-key)))
		       result)))
	  many-to-one-mappings :initial-value nil))


(defun plan-one-to-many (slot-definition class-name
			 root-primary-key foreign-key)
  (destructuring-bind (class-name &key table-name primary-key
				  properties one-to-many-mappings
				  many-to-one-mappings
				  superclass-mappings
				  subclass-mappings)
      (assoc class-name *mapping-schema* :key #'first)
    (let* ((alias (make-alias))
	   (table-join
	    (list :left-join table-name alias
		  (mapcar #'append root-primary-key
			  (apply #'append-alias alias foreign-key)))))
      (plan-class-mapping alias table-join primary-key properties
			  one-to-many-mappings many-to-one-mappings
			  superclass-mappings subclass-mappings))))

(defun plan-one-to-many-mappings (primary-key
				  &rest one-to-many-mappings)
  (reduce #'(lambda (result many-to-one-mapping)
	      (destructuring-bind (slot-definition
				   &key class-name foreign-key
				   serializer desirealizer)
		  many-to-one-mapping
		(acons slot-definition
		       #'(lambda ()
			   (apply #'plan-one-to-many slot-definition
				  class-name primary-key foreign-key))
		       result)))
	  many-to-one-mappings :initial-value nil))

(defun plan-superclass-slot-mappings (subclass-alias class-name
				      &key table-name primary-key
					foreign-key properties
					one-to-many-mappings
					many-to-one-mappings
					superclass-mappings)
  (let* ((alias
	  (make-alias))
	 (primary-key
	  (apply #'append-alias alias primary-key))
	 (foreign-key
	  (apply #'append-alias subclass-alias foreign-key)))
    (apply #'plan-superclasses-slot-mappings
	   (apply #'plan-properties alias properties)
	   (append
	    (apply #'plan-many-to-one-mappings
		   alias many-to-one-mappings)
	    (apply #'plan-one-to-many-mappings
		   primary-key one-to-many-mappings))
	   (list :inner-join table-name alias
		 (mapcar #'append primary-key foreign-key))
	   alias superclass-mappings)))

(defun plan-superclasses-slot-mappings (properties references
					table-join subclass-alias
					&optional superclass-mapping
					&rest superclass-mappings)
  (multiple-value-bind (rest-properties rest-references)
      (when (not (null superclass-mapping))
	(multiple-value-bind (properties references)
	    (apply #'plan-superclass-slot-mappings
		   subclass-alias superclass-mapping)
	  (apply #'plan-superclasses-slot-mappings properties
		 references table-join subclass-alias
		 superclass-mappings)))
    (values
     (append (append-join properties table-join)
	     rest-properties)
     (append (append-join references table-join)
	     rest-references))))

(defun plan-class-mapping (alias table-join primary-key properties
			   one-to-many-mappings many-to-one-mappings
			   superclass-mappings subclass-mappings)
  (let ((primary-key
	 (apply #'append-alias alias primary-key)))
    (multiple-value-bind (superclass-properties superclass-references)
	(apply #'plan-superclass-mappings
	       (apply #'plan-properties alias properties)
	       (append (apply #'plan-one-to-many-mappings
			      primary-key one-to-many-mappings)
		       (apply #'plan-many-to-one-mappings
			      alias many-to-one-mappings))
	       table-join alias superclass-mappings)
      (multiple-value-bind (all-references)
	  (apply #'plan-subclass-mappings references
		 table-join
		 (apply #'append-alias alias primary-key)
		 subclass-mappings)
	(values
	 #'(lambda (&optional (property-name nil name-present-p))
	     (if name-present-p
		 (rest (assoc property-name properties))))
	 #'(lambda (reference-name)
	     (rest (assoc refernce-name references)))
	 #'(lambda (reference-name)
	     (rest (assoc refernce-name all-references))))))))

(defun plan-subclass-mapping (superclass-primary-key class-name
			      &key table-name primary-key
				foreign-key properties
				one-to-many-mappings
				many-to-one-mappings
				superclass-mappings
				subclass-mappings)
  (let* ((alias
	  (make-alias))
	 (foreign-key
	  (apply #'append-alias alias foreign-key))
	 (table-join
	  (list* :inner-join table-name alias
		 (mapcar #'append superclass-primary-key foreign-key))))
    (plan-class-mapping alias table-join primary-key properties
			one-to-many-mappings many-to-one-mappings
			superclass-mappings subclass-mappings)))

(defun plan-subclass-mappings (references table-join
			       superclass-primary-key
			       &optional subclass-mapping
			       &rest subclass-mappings)
  (multiple-value-bind (subclass-references)
      (when (not (null subclass-mapping))
	(multiple-value-bind (subclass-references)
	    (apply #'plan-subclass-mapping
		   superclass-primary-key
		   subclass-mapping)
	  (apply #'plan-subclass-mappings
		 subclass-references
		 table-join
		 superclass-primary-key
		 subclass-mapping)))
    (append references
	    (apply #'append-join subclass-references table-join))))

(defun plan-root-class-mapping (alias class-name
				&key table-name primary-key properties
				  one-to-many-mappings many-to-one-mappings
				  superclass-mappings subclass-mappings)
  (let ((table-join (list table-name alias)))
    (plan-class-mapping alias table-join primary-key properties
			one-to-many-mappings many-to-one-mappings
			superclass-mappings subclass-mappings)))

(defun make-join-plan (class-mapping &rest class-mappings)
  (multiple-value-bind (rest-properties-and-loaders
			rest-join-references rest-fetch-references)
      (when (not (null class-mappings))
	(apply #'make-join-plan class-mappings))
    (multiple-value-bind (properties-and-loaders
			  join-references fetch-references)
	(apply #'plan-root-class-mapping (make-alias) class-mapping)
      (values
       (append properties-and-loaders rest-properties-and-loaders)
       (append join-references rest-join-references)
       (apprnd fetch-references rest-fetch-references)))))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

;; 1) Implement many roots
;; 2) Implement loaders

(defun db-read (roots &key select-list transform fetch-also where
			order-by having offset limit
			(mapping-schema *mapping-schema*))
  (declare (ignore select-list transform fetch-also where order-by
		   having offset limit))
  (let ((*table-index* 0))
    (multiple-value-bind (properties-and-loaders
			  join-references fetch-references)
	(if (not (listp roots))
	    (make-join-plan roots)
	    (apply #'make-join-plan roots))
      (list :properties-and-loaders properties-and-loaders
	    :join-references join-references
	    :fetch-references fetch-references))))
      ;;(class-mappings
      ;;(reduce #'(lambda (result class-name)
      ;;(list* (make-join-plan
      ;;(assoc class-name mapping-schema
      ;;:key #'first))
      ;;result))
	;;	  roots :initial-value nil)))
;;    class-mappings))
