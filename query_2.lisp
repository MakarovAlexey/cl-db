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

(defun plan-roots (&rest roots)
  (mapcar #'(lambda (class-name)
	      (
  (let ((class-mapping (class-mapping-of select-item))
	(path (binding-path select-item)))
    (apply #'plan-extension
	   (apply #'plan-inheritance
		  (plan-query query-plan select-item) class-mapping path)
	   class-mapping path))))))

(defun skip (&rest args) args)

(defun merge-trees (&rest trees)
  (reduce #'(lambda (result tree)
	      (list*
	       (list*
		(first tree)
		(apply #'merge-trees
		       (rest
			(append tree (rest
				      (assoc
				       (first tree) result))))))
	       (remove
		(first tree) result :key #'first)))
	  trees :initial-value nil))

(defun db-read (roots &key (join #'skip) where order-by having limit
		offset fetch single (mapping-schema *mapping-schema*))
  (let ((root-mappings
	 (mapcar #'(lambda (class-name)
		     (get-class-mapping (find-class class-name)
					mapping-schema))
		 (if (not (listp roots))
		     (list roots)
		     roots))))
    (multiple-value-list
     (apply join root-mappings))
	     


