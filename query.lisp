(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias ()
  (format nil "table_~a" (incf *table-index*)))

(defun plan-inheritance (persistent-class alias &optional superclass)
  (reduce #'(lambda (query-plan inheritance-mapping)
	      (let ((superclass-alias (make-alias))
		    (superclass (superclass-of inheritance-mapping)))
		(list*
		 (list :class superclass
		       :alias superclass-alias
		       :superclasses
		       (plan-inheritance superclass superclass-alias)
		       :join-on
		       (mapcar #'(lambda (fk-column pk-column)
				   (format nil "~a.~a" alias fk-column)
				   (format nil "~a.~a" superclass-alias pk-column))
			       (foreign-key-of inheritance-mapping)
			       (primary-key-of superclass)))
		 query-plan)))
	  (remove superclass (inheritance-mappings-of persistent-class) 
		  :key #'superclass-of)
	  :initial-value nil))

(defun plan-extension (persistent-class alias)
  (reduce #'(lambda (query-plan persistent-subclass)
	      (let ((subclass-alias (make-alias)))
		(list*
		 (list :class persistent-subclass
		       :alias subclass-alias
		       :superclasses
		       (plan-inheritance persistent-subclass
					 subclass-alias persistent-class)
		       :subclass (plan-extension persistent-subclass alias)
		       :join-on (mapcar #'(lambda (fk-column pk-column)
					    (format nil "~a.~a" subclass-alias fk-column)
					    (format nil "~a.~a" alias pk-column))
					(foreign-key-of persistent-subclass)
					(primary-key-of persistent-class)))
		 query-plan)))
	  (class-direct-subclasses persistent-class)
	  :initial-value nil))

(defun make-query (class)
  (let ((alias (make-alias)))
    (list :class class
	  :alias alias
	  :superclass (plan-inheritance class alias)
	  :subclass (plan-extension class alias))))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name)
  (let ((*table-index* 0)
	(class (find-class class-name)))
    (when (not (class-finalized-p class))
      (finalize-inheritance class))
    (make-query class)))
