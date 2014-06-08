(in-package #:cl-db)

;;(defun merge-trees (&rest trees)
;;  (reduce #'(lambda (result tree)
;;	      (list*
;;	       (list*
;;		(first tree)
;;		(apply #'merge-trees
;;		       (append
;;			(rest tree)
;;			(rest
;;			 (assoc
;;			  (first tree) result)))))
;;	       (remove (first tree) result :key #'first)))
;;	  trees :initial-value nil))

;;(defun make-loaders (class-mapping object-plan)
;;  (acons class-mapping object-plan
;;	 (reduce #'append
;;		 (extension-mappings-of class-mapping)
;;		 :key #'(lambda (extension-mapping)
;;			  (make-loaders
;;			   (subclass-mapping-of extension-mapping)
;;			   (assoc extension-mapping (rest object-plan)))))))

;;(defun compute-extension (class-mapping alias columns &rest superclasses)
;;  (list :class-mapping class-mapping
;;	:alias alias
;;	:columns columns
;;	:superclasses (mapcar #'(lambda (superclass)
;;				  (apply #'compute-inheritance superclass))
;;			      superclasses)))

;;(defun compute-extensions (extension &rest extensions)
;;  (list :extension (apply #'compute-extension extension)
;;	:extensions (mapcar #'(lambda (extension)
;;				(apply #'compute-extensions extension))
;;			    extensions)))

;;(defun compute-inheritance (class-mapping columns &rest superclasses)
;;  (list :class-mapping class-mapping
;;	:alias (sxhash class-mapping)
;;	:columns columns
;;	:superclasses (mapcar #'(lambda (superclass)
;;				  (apply #'compute-inheritance superclass))
;;			      superclasses)))

;;(defun compute-root (class-mapping &rest superclasses)
;;  (list :class-mapping class-mapping
;;	:alias (sxhash class-mapping)
;;	:superclasses (mapcar #'(lambda (superclass)
;;				  (apply #'compute-inheritance superclass))
;;			      superclasses)))

(defvar *table-index*)

(defun make-alias (&optional (table-index *table-index*))
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

(defun make-plan (root &rest extensions)
  (apply #'plan-extension
	 (list* class-mapping
		(make-alias (incf *table-index*))
		(apply #'plan-inheritance
		       (inheritance-mappings-of class-mapping)))
	 extensions))

(defun print-superclasses (stream object &optional colon-p at-sign-p)
  (format "INNER JOIN ~a as ~a ON ~{~}"

(defun print-root (stream object &optional colon-p at-sign-p)
  (apply #'(lambda (alias class-name table-name
		    &rest superclass-mappings)
	     (format stream "FROM ~a as ~a~%~{~/print-superclass/~}"
		     alias table-name (list* alias superclass-mappings)))
	 object))

;;(defun compute-from-clause (alias root &rest extensions)
;;  (format nil "~/pring-root/~{~/print-extension/~}"
;;	  (list* alias root)
;;	  extensions))

(defun fetch (root reference &rest references))

(defun join (class-names root reference &key (join #'skip) where order-by having))



(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let ((*table-index* 1)
	 (join-plan (apply #'make-plan
			   (get-class-mapping
			    (find-class class-name)
			    mapping-schema))))
    (apply #'compute-from-clause join-plan)))
