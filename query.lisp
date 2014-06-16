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

(defun make-alias (name)
  (format nil "~a_~a" name (incf *table-index*)))

(defun plan-inheritance (superclass-mapping &rest foreign-key)
  (list* (apply #'plan-class-mapping superclass-mapping)
	 foreign-key))

(defun plan-class-mapping (class-name table primary-key
			   &rest superclass-mappings)
  (list* class-name table (make-alias "table") primary-key
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'plan-inheritance superclass-mapping))
		 superclass-mappings)))

(defun plan-extension (extension-mapping &rest foreign-key)
  (list* (apply #'make-join-plan extension-mapping)
	 foreign-key))

(defun make-join-plan (class-mapping &rest extensions)
  (list*
   (apply #'plan-class-mapping class-mapping)
   (mapcar #'(lambda (extension-mapping)
	       (apply #'plan-extension extension-mapping))
	   extensions)))

(defun make-loaders (class-mapping object-plan)
  (acons class-mapping object-plan
	 (reduce #'append
		 (extension-mappings-of class-mapping)
		 :key #'(lambda (extension-mapping)
			  (make-loaders
			   (subclass-mapping-of extension-mapping)
			   (assoc extension-mapping (rest object-plan)))))))

(defun print-extension (class-mapping alias columns &rest superclasses)
  (list :class-mapping class-mapping
	:alias alias
	:columns columns
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'print-inheritance superclass))
			      superclasses)))

(defun print-extensions (extension &rest extensions)
  (list :extension (apply #'print-extension extension)
	:extensions (mapcar #'(lambda (extension)
				(apply #'print-extensions extension))
			    extensions)))

(defun print-inheritance (class-mapping columns &rest superclasses)
  (list :class-mapping class-mapping
	:alias (sxhash class-mapping)
	:columns columns
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'print-inheritance superclass))
			      superclasses)))

(defun print-root (class-mapping &rest superclasses)
  (list :class-mapping class-mapping
	:alias (sxhash class-mapping)
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'print-inheritance superclass))
			      superclasses)))

(defun print-from-clause (alias root &rest extensions)
  (list :root root
	:alias alias
	:extension extensions))

(defun fetch (root reference &rest references))

(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (assoc class-name (first mapping-schema) :key #'first))
	 (*table-index* 0))
    (apply #'print-from-clause
	   (apply #'make-join-plan class-mapping))))
