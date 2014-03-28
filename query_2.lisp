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
	   class-mapping path)))

(defun demo (a b &rest c)
  (list :a a :b b :c c))

(defun skip (join-plan) join-plan)

(defun plan-joins (join-plan &rest join-plans)
  (reduce #'merge-tree
	  (list* join-plan join-plans))) 

(defun db-read (roots &key join where order-by having limit offset
		fetch single (mapping-schema *mapping-schema*))
  (apply #'plan-joins
	 (apply (or join #'skip)
		(apply #'plan-roots roots)))