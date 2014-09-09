(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias &rest columns)
  (mapcar #'(lambda (column)
	      (format nil "~a.~a" alias column))
	  columns))

(defun plan-superclass-mappings (alias &rest superclass-mappings)
  (reduce #'(lambda (result superclass-mapping)
	      (append (apply #'(lambda (class-mapping &rest foreign-key)
				 (apply #'plan-superclass-mapping
					(apply #'append-alias
					       alias foreign-key)
					class-mapping))
			     superclass-mapping)
		      result))
	  superclass-mappings
	  :initial-value nil))

(defun plan-superclass-mapping (foreign-key class-name class-mapping
				&rest superclass-mappings)
  (declare (ignore class-name))
  (let ((alias (make-alias)))
    (list*
     (apply #'(lambda (&key table-name primary-key
			 &allow-other-keys)
		(list* :inner-join table-name :as alias
		       :on (pairlis foreign-key
				    (apply #'append-alias
					   alias primary-key))))
	    class-mapping)
     (apply #'plan-superclass-mappings alias superclass-mappings))))

(defun plan-subclass-mappings (superclass-primary-key
			       &rest subclass-mappings)
  (mapcar #'(lambda (subclass-mapping)
	      (apply #'(lambda (class-mapping &rest foreign-key)
			 (let ((alias (make-alias)))
			   (apply #'(lambda (first &rest rest)
				      (append (list* :left-join
						     (append first
							     (list* :on (pairlis superclass-primary-key
										 (apply #'append-alias
											alias foreign-key)))))
					      rest))
				  (apply #'plan-class-mapping alias class-mapping))))
		     subclass-mapping))
	  subclass-mappings))

(defun plan-class-mapping (alias class-mapping &rest subclass-mappings)
  (apply #'(lambda (class-name class-mapping &rest superclass-mappings)
	     (declare (ignore class-name))
	     (append
	      (apply #'(lambda (&key table-name primary-key
				  &allow-other-keys)
			 (list*
			  (list table-name :as alias)
			  (apply #'plan-subclass-mappings
				 (apply #'append-alias alias
					primary-key)
				 subclass-mappings)))
		     class-mapping)
	      (apply #'plan-superclass-mappings alias
		     superclass-mappings)))
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
