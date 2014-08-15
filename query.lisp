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

(defvar *table-index*)

(defun make-alias (&optional (prefix "table"))
  (format nil "~a_~a" prefix (incf *table-index*)))

(defun join-sublass (class &rest joined-classes)
  (let ((joined-superclasses
	 (reduce #'make-alias
		 (set-difference
		  (persistent-superclasses-of class)
		  (map #'first joined-classes))
		 :initial-value nil)))
    (list :left-join class
	  (
     ;; 2. inner-join new superclasses
     ;; 3. Pass modified joined-superclasses to subclasses

(defun make-query (class)
  (list*
   (list* class-mapping
	  (make-alias (incf *table-index*))
	  (apply #'plan-inheritance
		 (inheritance-mappings-of class-mapping)))
   (plan-extension class-mapping)))

;; планирование любого класса возващает два значения план и список
;; обработанных классов (с учетом уже присутствовавших ранее

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
				  (apply #'print-inheritance
			      superclass)) superclasses)))

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

(defun db-read (class-name)
  (let ((*table-index* 0)
	(class (find-class class-name)))
    (make-query class)))
