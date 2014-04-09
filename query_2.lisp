(in-package #:cl-db)

(defun plan-query (children node &rest path)
  (list*
   (list* node
	  (if (not (null path))
	      (apply #'plan-query
		     (rest (find node children :key #'first)) path)
	      (rest (find node children :key #'first))))
   (remove node children :key #'first)))

(defun plan-inheritance (query-plan class-mapping &rest path)
  (reduce #'(lambda (query-plan inheritance-mapping)
	      (let ((new-path (reverse
			       (list* inheritance-mapping
				      (reverse path)))))
		(apply #'plan-inheritance query-plan
		       (superclass-mapping-of inheritance-mapping)
		       new-path)))
	  (inheritance-mappings-of class-mapping)
	  :initial-value (apply #'plan-query query-plan path)))

(defun plan-extension (query-plan class-mapping &rest path)
  (reduce #'(lambda (query-plan extension-mapping)
	      (let ((new-path (reverse
			       (list* extension-mapping
				      (reverse path)))))
		(apply #'plan-extension
		       (apply #'plan-inheritance query-plan
			      class-mapping
			      new-path)
		       (subclass-mapping-of extension-mapping)
		       new-path)))
	  (extension-mappings-of class-mapping)
	  :initial-value (apply #'plan-query query-plan path)))

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
			  (first tree) result)))))))
	  (remove (first tree) result :key #'first)
	  trees :initial-value nil))

(defun fetch (root reference &rest references))

(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun plan-inheritance (class-mapping)
  (mapcar #'(lambda (inheritance-mapping)
	      (list* inheritance-mapping
		     (plan-inheritance
		      (superclass-mapping-of inheritance-mapping))))
	  (inheritance-mappings-of class-mapping)))

(defun plan-extension (class-mapping)
  (append
   (plan-inheritance class-mapping)
   (mapcar #'(lambda (extension-mapping)
	       (list* extension-mapping
		      (plan-extension
		       (subclass-mapping-of extension-mapping))))
	   (extension-mappings-of class-mapping))))

(defun plan-root (class-mapping)
  (list* class-mapping
	 (plan-extension class-mapping)))

(defun make-loaders (class-mapping join-plan)
  (acons class-mapping join-plan
	 (reduce #'append
		 (extension-mappings-of class-mapping)
		 :key #'(lambda (extension-mapping)
			  (make-loaders
			   (subclass-mapping-of extension-mapping)
			   (assoc extension-mapping (rest join-plan)))))))

(defun make-query (loaders join-plan)
  

(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (get-class-mapping (find-class class-name) mapping-schema))
	 (join-plan (plan-root class-mapping)))
    (make-loaders class-mapping join-plan)))