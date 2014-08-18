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

(defun make-alias (&optional (prefix "table"))
  (format nil "~a_~a" prefix (incf *table-index*)))

(defun plan-inheritance (&rest class-mapping
			 &key superclass-mappings &allow-other-keys)
  (list* :alias (make-alias)
	 :superclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'plan-inheritance superclass-mapping))
		 superclass-mappings)
	 (alexandria:remove-from-plist class-mapping
				       :superclass-mappings)))

(defun make-join-plan (&rest class-mapping &key superclass-mappings
					     subclass-mappings
					     &allow-other-keys)
  (list* :alias (make-alias)
	 :superclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'plan-inheritance superclass-mapping))
		 superclass-mappings)
	 :subclass-mappings
	 (mapcar #'(lambda (subclass-mapping)
		     (apply #'make-join-plan subclass-mapping))
		 subclass-mappings)
	 (alexandria:remove-from-plist class-mapping
				       :superclass-mappings
				       :subclass-mappings)))

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

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (find class-name mapping-schema
		:key #'(lambda (class-mapping)
			 (getf class-mapping :class-name))))
	 (*table-index* 0))
    (when (null class-mapping)
      (error "Mapping for class ~a not found" class-name))
    (apply #'make-join-plan class-mapping)))
