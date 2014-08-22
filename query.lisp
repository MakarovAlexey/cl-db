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

;; "свернуть" иерархию наследования
;; передавать свойства и ссылки сместе с псевдонимом (alias)

(defun plan-inheritance (&rest class-mapping &key (alias (make-alias))
					       superclass-mappings
					       &allow-other-keys)
  (list* :alias alias
	 :superclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'plan-inheritance superclass-mapping))
		 superclass-mappings)
	 (alexandria:remove-from-plist class-mapping
				       :superclass-mappings)))

(defun make-join-plan (&rest class-mapping &key (alias (make-alias))
					     superclass-mappings
					     subclass-mappings
					     &allow-other-keys)
  (list* :alias alias
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

(defun property-columns (alias property-name &rest columns)
  (declare (ignore property-name))
  (mapcar #'(lambda (column)
	      (destructuring-bind (name type) column
		(declare (ignore type))
		(cons alias name)))
	  columns))

(defun compute-query-superclass (&key table-name alias suclass-alias
				   foreign-key primary-key properties
				   superclass-mappings)
  (values
   (mapcar #'(lambda (property)
	       (apply #'property-columns alias property))
	   properties)))

(defun compute-query-superclasses (superclass-mapping
				   &rest superclass-mappings)
  (multiple-value-bind (select-clause from-clause)
      (apply #'compute-query-superclass superclass-mapping)
    (multiple-value-bind (rest-select-clause rest-from-clause)
	(when (not (null superclass-mappings))
	  (apply #'compute-query-superclasses superclass-mappings))
      (values
       (append select-clause rest-select-clause)
       (append from-clause rest-select-clause)))))

(defun make-query (class-mapping &key superclass-mappings
				   subclass-mappings
				   &allow-other-keys)
  (list :select (compute-superclassacons class-mapping object-plan
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

(defun append-to-query (query &key select from)
  (destructuring-bind (query-select query-from) query
    (list* :select (append query-select select)
	   :from (append 
  (alexandria:remove-from-plist class-mapping
				       :superclass-mappings)))

(defun from-clause (&key table-name alias superclass-mappings
		      subclass-mappings &allow-other-keys)
  (format nil (concatenate "FROM ~a AS ~a~%"
			   "~{~\print-superclass-join~\~}~%"
			   "~{~\print-subclass-join~\~}")
	  table-name alias superclass-mappings subclass-mappings))

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
