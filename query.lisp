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
  (list table (make-alias "table") primary-key
	(list* class-name
	       (mapcar #'(lambda (superclass-mapping)
			   (apply #'plan-inheritance superclass-mapping))
		       superclass-mappings))))

(defun plan-extension (extension-mapping &rest foreign-key)
  (list* (apply #'make-join-plan extension-mapping)
	 foreign-key))

(defun make-join-plan (class-mapping &rest extensions)
  (append
   (apply #'plan-class-mapping class-mapping)
   (mapcar #'(lambda (extension-mapping)
	       (apply #'plan-extension extension-mapping))
	   extensions)))

(defun print-superclass-mapping (root-alias foreign-key
				 table alias primary-key class-mapping)
  (list* :inner-join table :as alias
	 :on (pairlis
	      (mapcar #'(lambda (column)
			  (format nil "~a.~a" root-alias column))
		      foreign-key)
	      (mapcar #'(lambda (column)
			  (format nil "~a.~a" alias column))
		      primary-key))
	 (apply #'print-class-mapping
		alias class-mapping)))

(defun print-inheritance (alias class-mapping &rest foreign-key)
  (apply #'print-superclass-mapping
	 alias foreign-key class-mapping))

;; FIXME: добавить загрузку ассоциаций (понадобится первичный ключ)
(defun print-class-mapping (alias class-name &rest superclasses)
  (mapcar #'(lambda (superclass)
	      (apply #'print-inheritance alias superclass))
	  superclasses))

(defun print-subclass-mapping (parent-alias parent-primary-key
			       foreign-key table alias primary-key
			       class-mapping)
  (list* :left-join table :as alias
	 :on (pairlis
	      (mapcar #'(lambda (column)
			  (format nil "~a.~a" parent-alias column))
		      parent-primary-key)
	      (mapcar #'(lambda (column)
			  (format nil "~a.~a" alias column))
		      foreign-key))
	 (apply #'print-class-mapping alias class-mapping)))

(defun print-extension (parent-alias parent-primary-key
			class-mapping &rest foreign-key)
  (apply #'print-subclass-mapping
	 parent-alias parent-primary-key foreign-key class-mapping))

(defun print-from-clause (table alias primary-key root &rest extensions)
  (list* :from table :as alias
	 (append
	  (apply #'print-class-mapping alias root)
	  (mapcar #'(lambda (extension-mapping)
		      (apply #'print-extension alias
			     primary-key extension-mapping))
		  extensions))))

(defun make-loaders (class-mapping object-plan)
  (acons class-mapping object-plan
	 (reduce #'append
		 (extension-mappings-of class-mapping)
		 :key #'(lambda (extension-mapping)
			  (make-loaders
			   (subclass-mapping-of extension-mapping)
			   (assoc extension-mapping (rest object-plan)))))))

(defun fetch (root reference &rest references))

(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (assoc class-name (first mapping-schema) :key #'first))
	 (*table-index* 0))
    (apply #'print-from-clause
	   (apply #'make-join-plan class-mapping))))
