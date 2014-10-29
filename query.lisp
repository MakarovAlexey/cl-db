(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias &rest column-names)
  (mapcar #'(lambda (column-name)
	      (list alias column-name))
	  column-names))

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
			      loader))
			 result))))
	  properties :initial-value nil))

(defun plan-many-to-one (alias &rest many-to-one-mappings)
  (mapcar #'(lambda (many-to-one-mapping)
	      (destructuring-bind (slot-definition
				   class-name &rest foreign-key)
		  many-to-one-mapping
		(list slot-definition class-name alias
		      (apply #'append-alias foreign-key alias))))
	  many-to-one-mappings))

(defun plan-one-to-many (primary-key &rest one-to-many-mappings)
  (mapcar #'(lambda (one-to-many-mapping)
	      (destructuring-bind (slot-definition
				   class-name serializer desirealizer
				   &rest foreign-key)
		  one-to-many-mapping
		(let ((alias (make-alias)))
		  (list slot-definition class-name alias
			(mapcar #'append primary-key
				(append-alias alias foreign-key))))))
	  one-to-many-mappings))

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

(defun plan-subclass-mapping (subclass-alias class-name
			      &key table-name primary-key
				foreign-key properties
				one-to-many-mappings
				many-to-one-mappings
				superclass-mappings)
  (let* ((foreign-key
	  (apply #'append-alias subclass-alias foreign-key))
	 (alias
	  (make-alias))
	 (primary-key
	  (apply #'append-alias alias primary-key)))
    (multiple-value-bind (super-from-clause super-properties
					    super-one-to-many-mappings
					    super-many-to-one-mappings)
	(apply #'plan-superclass-mappings alias superclass-mappings)
      (values
       (list* :inner-join table-name alias
	      (mapcar #'append primary-key foreign-key)
	      super-from-clause)
       (append (apply #'plan-properties alias properties)
	       super-properties)
       (append (apply #'plan-one-to-many-mappings
		      primary-key one-to-many-mappings)
	       super-one-to-many-mappings)
       (append (apply #'plan-many-to-one-mappings
		      alias many-to-one-mappings)
	       super-many-to-one-mappings)))))

(defun plan-subclass-mappings (superclass-primary-key subclass-mapping
			       &rest subclass-mappings)
  (multiple-value-bind (from-clause-list property-list
					 one-to-many-mapping-list
					 many-to-one-mapping-list)
      (when (not (null subclass-mappings))
	(apply #'plan-subclass-mappings
	       superclass-primary-key subclass-mappings))
    (multiple-value-bind (from-clause properties
				      one-to-many-mappings
				      many-to-one-mappings)
	(apply #'plan-subclass-mapping
	       superclass-primary-key subclass-mapping)
      (values
       (append from-clause from-clause-list)
       (append properties property-list)
       (append one-to-many-mappings one-to-many-mapping-list)
       (append many-to-one-mappings many-to-one-mapping-list)))))

;;(multiple-value-bind (sub-from-clause sub-properties
;;				      sub-one-to-many-mappings
;;				      sub-many-to-one-mappings)
;;    (when (not (null plan-subclass-p))
;;      (apply #'plan-subclass-mappings primary-key
;;	     subclass-mappings))

(defun plan-class-mapping (alias class-name
			   &key table-name primary-key properties
			     one-to-many-mappings many-to-one-mappings
			     superclass-mappings subclass-mappings)
  (apply #'plan-superclass-mappings
	 (apply #'plan-properties alias properties)
	 (append
	  (apply #'plan-many-to-one-mappings alias
		 many-to-one-mappings)
	  (apply #'plan-one-to-many-mappings
		 (apply #'append-alias alias primary-key)
		 one-to-many-mappings))
	 (list table-name alias)
	 alias superclass-mappings))

(defun make-join-plan (class-mapping)
  (plan-class-mapping (make-alias) class-mapping))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &key select-list transform fetch-also where
			     order-by having offset limit
			     (mapping-schema *mapping-schema*))
  (declare (ignore select-list transform fetch-also where order-by
		   having offset limit))
  (let* ((class-mapping
	  (assoc class-name mapping-schema :key #'first))
	 (*table-index* 0))
    (multiple-value-bind (properties )
	(plan-class-mapping (make-alias) class-mapping)
    (make-join-plan class-mapping
