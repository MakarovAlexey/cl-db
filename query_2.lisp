(in-package #:cl-db)

(defun merge-trees (&rest trees)
  (reduce #'(lambda (result tree)
	      (list*
	       (list*
		(first tree)
		(apply #'merge-trees
		       (append
			(rest tree)
			(rest
			 (assoc
			  (first tree) result)))))
	       (remove (first tree) result :key #'first)))
	  trees :initial-value nil))

(defvar *table-index*)

(defun make-alias (table-index)
  (format nil "table_~a" table-index))

(defun plan-inheritance (&rest inheritance-mappings)
  (mapcar #'(lambda (inheritance-mapping)
	      (list*
	       (superclass-mapping-of inheritance-mapping)
	       (make-alias (incf *table-index*))
	       (columns-of inheritance-mapping)
	       (apply  #'plan-inheritance
		       (superclass-mapping-of inheritance-mapping))))
	  inheritance-mappings))

(defun plan-extension (class-mapping &optional root-superclass)
  (mapcar #'(lambda (extension-mapping)
	      (list*
	       (list*
		(subclass-mapping-of extension-mapping)
		(make-alias (incf *table-index*))
		(columns-of extension-mapping)
		(apply #'plan-inheritance
		       (remove root-superclass
			       (inheritance-mappings-of
				(subclass-mapping-of extension-mapping)))))
	       (plan-extension
		(subclass-mapping-of extension-mapping) class-mapping)))
	  (extension-mappings-of class-mapping)))

(defun make-join-plan (class-mapping)
  (list*
   (list* class-mapping
	  (make-alias (incf *table-index*))
	  (apply #'plan-inheritance
		 (inheritance-mappings-of class-mapping)))
   (plan-extension class-mapping)))

(defun make-loaders (class-mapping object-plan)
  (acons class-mapping object-plan
	 (reduce #'append
		 (extension-mappings-of class-mapping)
		 :key #'(lambda (extension-mapping)
			  (make-loaders
			   (subclass-mapping-of extension-mapping)
			   (assoc extension-mapping (rest object-plan)))))))

(defun compute-extension (class-mapping alias columns &rest superclasses)
  (list :class-mapping class-mapping
	:alias alias
	:columns columns
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'compute-inheritance superclass))
			      superclasses)))

(defun compute-extensions (extension &rest extensions)
  (list :extension (apply #'compute-extension extension)
	:extensions (mapcar #'(lambda (extension)
				(apply #'compute-extensions extension))
			    extensions)))

(defun compute-inheritance (class-mapping columns &rest superclasses)
  (list :class-mapping class-mapping
	:alias (sxhash class-mapping)
	:columns columns
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'compute-inheritance superclass))
			      superclasses)))

(defun compute-root (class-mapping &rest superclasses)
  (list :class-mapping class-mapping
	:alias (sxhash class-mapping)
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'compute-inheritance superclass))
			      superclasses)))

(defun compute-from-clause (alias root &rest extensions)
  (list :root root
	:alias alias
	:extension extensions))

(defun fetch (root reference &rest references))

(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (get-class-mapping (find-class class-name) mapping-schema))
	 (*table-index* 0)
	 (join-plan (make-join-plan class-mapping)))
    (apply #'compute-from-clause join-plan)))
