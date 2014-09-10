(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias &rest columns)
  (mapcar #'(lambda (column)
	      (format nil "~a.~a" alias column))
	  columns))

(defun plan-superclass-mappings (alias &optional superclass-mapping
				 &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (multiple-value-bind (from-clause)
	(apply #'(lambda (class-mapping &rest foreign-key)
		   (apply #'plan-superclass-mapping
			  (apply #'append-alias
				 alias foreign-key)
			  class-mapping))
	       superclass-mapping)
      (multiple-value-bind (superclass-from-clause)
	  (apply #'plan-superclass-mappings alias superclass-mappings)
	(append from-clause superclass-from-clause)))))

(defun plan-superclass-mapping (foreign-key class-name class-mapping
				&rest superclass-mappings)
  (declare (ignore class-name))
  (let ((alias (make-alias)))
    (multiple-value-bind (superclass-from-clause)
	(apply #'plan-superclass-mappings alias superclass-mappings)
      (apply #'(lambda (&key table-name primary-key
			  &allow-other-keys)
		 (list* (list* :inner-join table-name :as alias
			       :on (pairlis foreign-key
					    (apply #'append-alias
						   alias primary-key)))
			superclass-from-clause))
	     class-mapping))))

(defun plan-subclass-mappings (superclass-primary-key
			       &optional subclass-mapping
			       &rest subclass-mappings)
  (when (not (null subclass-mapping))
    (multiple-value-bind (from-clause)
	(apply #'(lambda (class-mapping &rest foreign-key)
		   (let ((alias (make-alias)))
		     (multiple-value-bind (from-clause)
			 (apply #'plan-class-mapping alias class-mapping)
		       (apply #'(lambda (first &rest rest)
				  (list* (list* :left-join
						(append first
							(list* :on (pairlis superclass-primary-key
									    (apply #'append-alias
										   alias foreign-key)))))
					 rest))
			      from-clause))))
	       subclass-mapping)
      (multiple-value-bind (subclass-from-clause)
	  (apply #'plan-subclass-mappings
		 superclass-primary-key subclass-mappings)
	(append from-clause subclass-from-clause)))))

(defun plan-class-mapping (alias class-mapping &rest subclass-mappings)
  (apply #'(lambda (class-name class-mapping &rest superclass-mappings)
	     (declare (ignore class-name))
	     (multiple-value-bind (from-clause)
		 (apply #'(lambda (&key table-name primary-key
				     &allow-other-keys)
			    (multiple-value-bind (from-clause)
				(apply #'plan-subclass-mappings
				       (apply #'append-alias alias
					      primary-key)
				       subclass-mappings)
			      (list* (list table-name :as alias)
				     from-clause)))
			class-mapping)
	       (multiple-value-bind (superclasses-from-clause)
		   (apply #'plan-superclass-mappings alias
			  superclass-mappings)
		 (append from-clause superclasses-from-clause))))
	 class-mapping))

(defun make-join-plan (class-mapping)
  (apply #'plan-class-mapping (make-alias) class-mapping))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &key (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (assoc class-name mapping-schema :key #'first))
	 (*table-index* 0))
    (make-join-plan class-mapping)))
