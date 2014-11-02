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
		 (mapcar #'append superclass-primary-key foreign-key)))
	 (primary-key
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

(defun plan-class-mapping (alias class-name
			   &key table-name primary-key properties
			     one-to-many-mappings many-to-one-mappings
			     superclass-mappings subclass-mappings)
  (let ((table-join (list table-name alias)))
    (multiple-value-bind (properties references)
	(apply #'plan-superclass-mappings
	       (apply #'plan-properties alias properties)
	       (append
		(apply #'plan-many-to-one-mappings alias
		       many-to-one-mappings)
		(apply #'plan-one-to-many-mappings
		       (apply #'append-alias alias primary-key)
		       one-to-many-mappings))
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

(defun make-join-plan (class-mapping)
  (plan-class-mapping (make-alias) class-mapping))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (roots &key select-list transform fetch-also where
			order-by having offset limit
			(mapping-schema *mapping-schema*))
  (declare (ignore select-list transform fetch-also where order-by
		   having offset limit))
  (let* ((*table-index* 0)
	 (class-mappings
	  (reduce #'(lambda (result class-name)
		      (list* (make-join-plan
			      (assoc class-name mapping-schema
				     :key #'first))
			     result))
		  roots :initial-value nil)))
    class-mappings))
